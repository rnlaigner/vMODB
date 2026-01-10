package dk.ku.di.dms.vms.web_common;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.WARNING;

public abstract class ModbHttpServer extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(ModbHttpServer.class.getName());

    private static final ExecutorService BACKGROUND_EXECUTOR = Executors.newSingleThreadExecutor();

    protected static final List<HttpReadCompletionHandler> SSE_CLIENTS = new CopyOnWriteArrayList<>();

    // must be concurrent because it is accessed by different threads
    protected static final List<BiConsumer<Long, Long>> BATCH_COMMIT_CONSUMERS = new CopyOnWriteArrayList<>();

    private static final Set<Future<?>> TRACKED_FUTURES = ConcurrentHashMap.newKeySet();

    static {
        // register client as a batch commit consumer
        BATCH_COMMIT_CONSUMERS.add( (_, numTIDsCommitted) -> {
            for (var sseClient : SSE_CLIENTS){
                sseClient.sendToSseClient(numTIDsCommitted);
            }
        });
    }

    protected static void submitBackgroundTask(Runnable task){
        TRACKED_FUTURES.add(BACKGROUND_EXECUTOR.submit(task));
    }

    protected static void cancelBackgroundTasks(){
        for (Future<?> future : TRACKED_FUTURES) {
            future.cancel(false);
        }
        TRACKED_FUTURES.clear();
    }

    protected static final class HttpReadCompletionHandler implements CompletionHandler<Integer, HttpReadCompletionHandler.RequestTracking> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;
        private final ByteBuffer writeBuffer;
        private final IHttpHandler httpHandler;

        private final DefaultWriteCH defaultWriteCH = new DefaultWriteCH();
        private final CloseWriteCH closeWriteCH = new CloseWriteCH();
        private final BigBbWriteCH bigBbWriteCH = new BigBbWriteCH();
        private final SseWriteCH sseWriteCH;

        // tracking of current client request
        public static final class RequestTracking {
            public final StringBuilder requestBuilder;
            public volatile boolean headersParsed = false;
            public volatile int contentLength = 0;
            public volatile int bodyBytesRead = 0;
            public volatile String method;
            public volatile String accept;
            public volatile String uri;
            public RequestTracking() {
                this.requestBuilder = new StringBuilder();
            }
        }

        public HttpReadCompletionHandler(ConnectionMetadata connectionMetadata,
                                         ByteBuffer readBuffer, ByteBuffer writeBuffer,
                                         IHttpHandler httpHandler) {
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            this.writeBuffer = writeBuffer;
            this.httpHandler = httpHandler;
            this.sseWriteCH = new SseWriteCH(this);
        }

        private static String createHttpHeaders(int contentLength) {
            return "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: " + contentLength + "\r\n" +
                    "Connection: keep-alive\r\n\r\n";
        }

        private static final byte[] OK_RESPONSE_BYTES = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.UTF_8);

        private static final byte[] ERROR_RESPONSE_BYTES =
                ("HTTP/1.1 400 Bad Request\r\n"
                + "Content-Type: text/plain\r\n"
                + "Content-Length: 36\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "Accept header value is not supported").getBytes(StandardCharsets.UTF_8);

        private static final byte[] NO_ACCEPT_IN_HEADER_ERR_MSG =
                ("HTTP/1.1 400 Bad Request\r\n"
                + "Content-Type: text/plain\r\n"
                + "Content-Length: 24\r\n"
                + "Connection: close\r\n"
                + "\r\n"
                + "No accept type in header").getBytes(StandardCharsets.UTF_8);

        private static final byte[] NO_PAYLOAD_IN_BODY_ERR_MSG =
                ("HTTP/1.1 400 Bad Request\r\n"
                        + "Content-Type: text/plain\r\n"
                        + "Content-Length: 22\r\n"
                        + "Connection: close\r\n"
                        + "\r\n"
                        + "No payload in the body").getBytes(StandardCharsets.UTF_8);

        private static final byte[] NOT_FOUND_ERR_MSG =
                ("HTTP/1.1 404 Not Found\r\n"
                        + "Content-Type: text/plain\r\n"
                        + "Content-Length: 30\r\n"
                        + "Connection: close\r\n"
                        + "\r\n"
                        + "Object requested was not found").getBytes(StandardCharsets.UTF_8);

        public void parse(RequestTracking requestTracking){
            this.readBuffer.rewind();
            byte[] data = new byte[this.readBuffer.remaining()];
            this.readBuffer.get(data);
            this.readBuffer.clear();
            requestTracking.requestBuilder.append(new String(data, StandardCharsets.UTF_8));

            if (!requestTracking.headersParsed) {
                int headersEnd = requestTracking.requestBuilder.indexOf("\r\n\r\n");
                if (headersEnd != -1) {
                    requestTracking.headersParsed = true;
                    String headersPart = requestTracking.requestBuilder.substring(0, headersEnd);
                    String[] lines = headersPart.split("\r\n");

                    String[] requestParts = lines[0].split(" ");
                    requestTracking.method = requestParts[0];
                    requestTracking.uri = requestParts[1];

                    for (int i = 1; i < lines.length; i++) {
                        var lineLC = lines[i].toLowerCase();
                        if (lineLC.startsWith("content-length:")) {
                            requestTracking.contentLength = Integer.parseInt(lines[i].split(":")[1].trim());
                        }
                        if(lineLC.startsWith("accept:")){
                            requestTracking.accept = lines[i].substring(7).trim();
                        }
                    }
                    requestTracking.bodyBytesRead = requestTracking.requestBuilder.length() - (headersEnd + 4);
                    if (requestTracking.bodyBytesRead >= requestTracking.contentLength) {
                        this.process(requestTracking.method,
                                requestTracking.accept,
                                requestTracking.uri,
                                requestTracking.requestBuilder.substring(headersEnd+4));
                        return;
                    }

                    // java http client may be miscalculating content length
                    // if it is a well-formed JSON, then it is safe to proceed
                    // e.g., if(requestTracking.contentLength - requestTracking.bodyBytesRead == 1)

                }
            } else {
                int newBodyByteRead = requestTracking.bodyBytesRead;
                requestTracking.bodyBytesRead = newBodyByteRead + data.length;
                if (requestTracking.bodyBytesRead  >= requestTracking.contentLength) {
                    int headersEnd = requestTracking.requestBuilder.indexOf("\r\n\r\n");
                    this.process(requestTracking.method,
                            requestTracking.accept,
                            requestTracking.uri,
                            requestTracking.requestBuilder.substring(headersEnd+4));
                    return;
                }
            }
            this.connectionMetadata.channel.read(this.readBuffer, requestTracking, this);
        }

        public void process(String httpMethod,
                            String accept,
                            String uri,
                            String body){
            try {
                switch (httpMethod){
                    case "GET" -> {
                        if(accept == null || accept.isBlank()){
                            this.sendErrorMsgAndCloseConnection(NO_ACCEPT_IN_HEADER_ERR_MSG);
                            return;
                        }
                        switch (accept) {
                            case "*/*", "application/json" -> ForkJoinPool.commonPool().submit(() -> {
                                Object objectJson = this.httpHandler.getAsJson(uri);

                                if(objectJson == null){
                                    try {
                                        this.sendErrorMsgAndCloseConnection(NOT_FOUND_ERR_MSG);
                                        return;
                                    } catch (Exception e){
                                        this.processError(body, e);
                                        return;
                                    }
                                }

                                byte[] dashJsonBytes = objectJson.toString().getBytes(StandardCharsets.UTF_8);
                                String headers = createHttpHeaders(dashJsonBytes.length);
                                byte[] headerBytes = headers.getBytes(StandardCharsets.UTF_8);
                                // ask memory utils for a byte buffer big enough to fit the seller dashboard
                                int totalBytes = headerBytes.length + dashJsonBytes.length;
                                // use remaining to be error-proof
                                if (this.writeBuffer.remaining() < totalBytes) {
                                    ByteBuffer bigBB = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.nextPowerOfTwo(totalBytes));
                                    bigBB.put(headerBytes);
                                    bigBB.put(dashJsonBytes);
                                    bigBB.flip();
                                    this.connectionMetadata.channel.write(bigBB, bigBB, this.bigBbWriteCH);
                                } else {
                                    if (this.writeBuffer.position() != 0) {
                                        LOGGER.log(ERROR, "This buffer has not been cleaned appropriately!");
                                        this.writeBuffer.clear();
                                    }
                                    this.writeBuffer.put(headerBytes);
                                    this.writeBuffer.put(dashJsonBytes);
                                    this.writeBuffer.flip();
                                    this.connectionMetadata.channel.write(this.writeBuffer, null, this.defaultWriteCH);
                                }
                            });
                            case "application/octet-stream" -> {
                                byte[] byteArray = this.httpHandler.getAsBytes(uri);
                                String headers = "HTTP/1.1 200 OK\r\nContent-Length: " + byteArray.length +
                                        "\r\nContent-Type: application/octet-stream\r\n\r\n";
                                this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
                                this.writeBuffer.put(byteArray);
                                this.writeBuffer.flip();
                                this.connectionMetadata.channel.write(this.writeBuffer, null, this.defaultWriteCH);
                            }
                            case "text/event-stream" -> {
                                this.processSseClient();
                                return;
                            }
                            default -> {
                                this.sendErrorMsgAndCloseConnection(ERROR_RESPONSE_BYTES);
                                return;
                            }
                        }
                    }
                    case "POST" -> {
                        if(body.isEmpty()){
                            this.sendErrorMsgAndCloseConnection(NO_PAYLOAD_IN_BODY_ERR_MSG);
                            return;
                        }
                        this.httpHandler.post(uri, body);
                        if(this.writeBuffer.remaining() < OK_RESPONSE_BYTES.length){
                            LOGGER.log(WARNING, "Write buffer has no sufficient space. Did you forget to clean it up?");
                            writeBuffer.clear();
                        }
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer, null, defaultWriteCH);
                    }
                    case "PATCH" -> {
                        if(uri.contains("reset")) {
                            cancelBackgroundTasks();
                        }
                        this.httpHandler.patch(uri, body);
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer, null, defaultWriteCH);
                    }
                    case "PUT" -> {
                        this.httpHandler.put(uri, body);
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer, null, defaultWriteCH);
                    }
                }
                this.readBuffer.clear();
                this.connectionMetadata.channel.read(
                        this.readBuffer,
                        new RequestTracking(),
                        this);
            } catch (Exception e){
                this.processError(body, e);
            }
        }

        private void processError(String request, Exception e) {
            this.writeBuffer.clear();
            byte[] errorBytes;
            if(e.getMessage() == null || e.getMessage().isEmpty()){
                LOGGER.log(ERROR, "Exception without message has been caught:\n"+ e +"\nRequest:\n"+ request);
                e.printStackTrace(System.out);
                errorBytes = ERROR_RESPONSE_BYTES;
            } else {
                errorBytes = ("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: "+
                        e.getMessage().length()+"\r\n\r\n" + e.getMessage()).getBytes(StandardCharsets.UTF_8);
            }
            this.writeBuffer.put(errorBytes);
            this.writeBuffer.flip();
            this.connectionMetadata.channel.write(this.writeBuffer, null, this.defaultWriteCH);
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, new RequestTracking(), this);
        }

        private void sendErrorMsgAndCloseConnection(byte[] errorResponseBytes) throws IOException {
            this.writeBuffer.put(errorResponseBytes);
            this.writeBuffer.flip();
            this.connectionMetadata.channel.write(this.writeBuffer, null, this.closeWriteCH);
            this.readBuffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
            this.connectionMetadata.channel.close();
        }

        private void processSseClient() throws InterruptedException, ExecutionException {
            // Write HTTP response headers for SSE
            String headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n";
            this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            do {
                this.connectionMetadata.channel.write(this.writeBuffer).get();
            } while(this.writeBuffer.hasRemaining());
            // need to set up read before adding this connection to sse client
            this.writeBuffer.clear();
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, null, this);
        }

        public void sendToSseClient(long numTIDsCommitted){
            String eventData = "data: " + numTIDsCommitted + "\n\n";
            this.writeBuffer.put(eventData.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            try {
                this.connectionMetadata.channel.write(this.writeBuffer, null, this.sseWriteCH);
            } catch (Exception e) {
                LOGGER.log(ERROR, "Error caught: "+e.getMessage());
                SSE_CLIENTS.remove(this);
                this.writeBuffer.clear();
            }
        }

        @Override
        public void completed(Integer result, RequestTracking requestTracking) {
            if(result == -1){
                this.readBuffer.clear();
                this.writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
                MemoryManager.releaseTemporaryDirectBuffer(this.writeBuffer);
                // LOGGER.log(DEBUG,me.identifier+": HTTP client has disconnected!");
                SSE_CLIENTS.remove(this);
                return;
            }
            this.readBuffer.flip();
            this.parse(requestTracking);
        }

        @Override
        public void failed(Throwable exc, RequestTracking requestTracking) {
            LOGGER.log(ERROR, "Error captured: \n"+exc);
            try {
                this.connectionMetadata.channel.close();
            } catch (Exception ignored){}
            finally {
                this.readBuffer.clear();
            }
            // this.connectionMetadata.channel.read(this.readBuffer, null, this);
        }

        private final class CloseWriteCH implements CompletionHandler<Integer, Void> {
            @Override
            public void completed(Integer result, Void ignored) {
                if(writeBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(writeBuffer, null, this);
                    return;
                }
                writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(writeBuffer);
            }
            @Override
            public void failed(Throwable exc, Void ignored) {
                writeBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(writeBuffer);
            }
        }

        private final class DefaultWriteCH implements CompletionHandler<Integer, Void> {
            @Override
            public void completed(Integer result, Void ignored) {
                if(writeBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(writeBuffer, null, this);
                    return;
                }
                writeBuffer.clear();
            }
            @Override
            public void failed(Throwable exc, Void ignored) {
                writeBuffer.clear();
            }
        }

        private final class SseWriteCH implements CompletionHandler<Integer, Void> {
            private final HttpReadCompletionHandler r;
            public SseWriteCH(HttpReadCompletionHandler r){
                this.r = r;
            }
            @Override
            public void completed(Integer result, Void ignored) {
                if(writeBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(writeBuffer, null, this);
                    return;
                }
                writeBuffer.clear();
            }
            @Override
            public void failed(Throwable exc, Void ignored) {
                writeBuffer.clear();
                SSE_CLIENTS.remove(this.r);
            }
        }

        private final class BigBbWriteCH implements CompletionHandler<Integer, ByteBuffer> {
            @Override
            public void completed(Integer result, ByteBuffer byteBuffer) {
                if(byteBuffer.hasRemaining()) {
                    connectionMetadata.channel.write(byteBuffer, byteBuffer, this);
                    return;
                }
                byteBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(byteBuffer);
            }
            @Override
            public void failed(Throwable exc, ByteBuffer byteBuffer) {
                byteBuffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(byteBuffer);
            }
        }
    }

    /**
     * Useful for managed experiments, that is, when workload is generated in place
     */
    public void registerBatchCommitConsumer(BiConsumer<Long, Long> consumer){
        BATCH_COMMIT_CONSUMERS.add(consumer);
    }

}

