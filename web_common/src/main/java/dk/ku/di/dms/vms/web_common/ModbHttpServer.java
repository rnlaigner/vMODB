package dk.ku.di.dms.vms.web_common;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

public abstract class ModbHttpServer extends StoppableRunnable {

    protected static final List<HttpReadCompletionHandler> SSE_CLIENTS = new CopyOnWriteArrayList<>();

    private static final ExecutorService BACKGROUND_EXECUTOR = Executors.newSingleThreadExecutor();

    private static final Set<Future<?>> TRACKED_FUTURES = ConcurrentHashMap.newKeySet();

    protected static void submitBackgroundTask(Runnable task){
        TRACKED_FUTURES.add(BACKGROUND_EXECUTOR.submit(task));
    }

    protected static void cancelBackgroundTasks(){
        for (Future<?> future : TRACKED_FUTURES) {
            future.cancel(false);
        }
        TRACKED_FUTURES.clear();
    }

    protected static final class HttpReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;
        private final ByteBuffer writeBuffer;
        private final IHttpHandler httpHandler;

        public HttpReadCompletionHandler(ConnectionMetadata connectionMetadata,
                                         ByteBuffer readBuffer, ByteBuffer writeBuffer,
                                         IHttpHandler httpHandler) {
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            this.writeBuffer = writeBuffer;
            this.httpHandler = httpHandler;
        }

        private record HttpRequestInternal(String httpMethod, Map<String, String> headers, String uri, String body) {}

        private static HttpRequestInternal parseRequest(String request){
            String[] requestLines = request.split("\r\n");
            String requestLine = requestLines[0];  // First line is the request line
            String[] requestLineParts = requestLine.split(" ");
            String method = requestLineParts[0];
            String url = requestLineParts[1];
            String httpVersion = requestLineParts[2];
            // process header
            Map<String, String> headers = new HashMap<>();
            int i = 1;
            while (requestLines.length > i && !requestLines[i].isEmpty()) {
                String[] headerParts = requestLines[i].split(": ");
                headers.put(headerParts[0], headerParts[1]);
                i++;
            }
            if(method.contentEquals("GET")){
                return new HttpRequestInternal(method, headers, url, "");
            }
            StringBuilder body = new StringBuilder();
            for (i += 1; i < requestLines.length; i++) {
                body.append(requestLines[i]).append("\r\n");
            }
            String payload = body.toString().trim();
            return new HttpRequestInternal(method, headers, url, payload);
        }

        private static String createHttpHeaders(int contentLength) {
            return "HTTP/1.1 200 OK\r\n" +
                    "Content-Type: application/json\r\n" +
                    "Content-Length: " + contentLength + "\r\n" +
                    "Connection: keep-alive\r\n\r\n";
        }

        public void process(String request){
            try {
                HttpRequestInternal httpRequest = parseRequest(request);
                switch (httpRequest.httpMethod()){
                    case "GET" -> {
                        if(!httpRequest.headers.containsKey("Accept")){
                            var noContentType = "HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: 11\r\n\r\nNo accept type in header".getBytes(StandardCharsets.UTF_8);
                            this.writeBuffer.put(noContentType);
                        } else {
                            switch (httpRequest.headers.get("Accept")) {
                                case "*/*", "application/json" -> {
                                    String dashJson = httpHandler.getAsJson(httpRequest.uri());
                                    byte[] dashJsonBytes = dashJson.getBytes(StandardCharsets.UTF_8);
                                    String headers = createHttpHeaders(dashJsonBytes.length);
                                    byte[] headerBytes = headers.getBytes(StandardCharsets.UTF_8);
                                    // ask memory utils for a byte buffer big enough to fit the seller dashboard
                                    int totalBytes = headerBytes.length + dashJsonBytes.length;
                                    // use remaining to be error-proof
                                    if(this.writeBuffer.remaining() < totalBytes) {
                                        ByteBuffer bigBB = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.nextPowerOfTwo(totalBytes));
                                        bigBB.put(headerBytes);
                                        bigBB.put(dashJsonBytes);
                                        bigBB.flip();
                                        this.connectionMetadata.channel.write(bigBB);
                                        bigBB.clear();
                                        MemoryManager.releaseTemporaryDirectBuffer(bigBB);
                                    } else {
                                        if(this.writeBuffer.position() != 0){
                                            System.out.println("This buffer has not been cleaned appropriately!");
                                            this.writeBuffer.clear();
                                        }
                                        this.writeBuffer.put(headerBytes);
                                        this.writeBuffer.put(dashJsonBytes);
                                        this.writeBuffer.flip();
                                        this.connectionMetadata.channel.write(this.writeBuffer);
                                    }
                                }
                                case "application/octet-stream" -> {
                                    byte[] byteArray = httpHandler.getAsBytes(httpRequest.uri());
                                    String headers = "HTTP/1.1 200 OK\r\nContent-Length: " + byteArray.length +
                                            "\r\nContent-Type: application/octet-stream\r\n\r\n";
                                    this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
                                    this.writeBuffer.put(byteArray);
                                    this.writeBuffer.flip();
                                    this.connectionMetadata.channel.write(this.writeBuffer);
                                }
                                case "text/event-stream" -> {
                                    this.processSseClient();
                                    return;
                                }
                                case null, default -> {
                                    this.writeBuffer.put(ERROR_RESPONSE_BYTES);
                                    this.writeBuffer.flip();
                                    this.connectionMetadata.channel.write(this.writeBuffer);
                                }
                            }
                        }
                    }
                    case "POST" -> {
                        httpHandler.post(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                    case "PATCH" -> {
                        if(httpRequest.uri().contains("reset")) {
                            cancelBackgroundTasks();
                        }
                        httpHandler.patch(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                    case "PUT" -> {
                        httpHandler.put(httpRequest.uri(), httpRequest.body());
                        this.writeBuffer.put(OK_RESPONSE_BYTES);
                        this.writeBuffer.flip();
                        this.connectionMetadata.channel.write(this.writeBuffer);
                    }
                }
                assert this.writeBuffer.position() == this.writeBuffer.limit();
                this.writeBuffer.clear();
                this.readBuffer.clear();
                this.connectionMetadata.channel.read(this.readBuffer, 0, this);
            } catch (Exception e){
                // LOGGER.log(WARNING, me.identifier+": Error caught in HTTP handler.\n"+e);
                this.writeBuffer.clear();
                byte[] errorBytes;
                if(e.getMessage() == null){
                    System.out.println("Exception without message has been caught:\n"+e);
                    e.printStackTrace(System.out);
                    errorBytes = ERROR_RESPONSE_BYTES;
                } else {
                    errorBytes = ("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: "+
                            e.getMessage().length()+"\r\n\r\n" + e.getMessage()).getBytes(StandardCharsets.UTF_8);
                }
                this.writeBuffer.put(errorBytes);
                this.writeBuffer.flip();
                try {
                    this.connectionMetadata.channel.write(this.writeBuffer);
                } catch (Exception ignored) {}
                this.writeBuffer.clear();
                this.readBuffer.clear();
                this.connectionMetadata.channel.read(this.readBuffer, 0, this);
            }
        }

        private void processSseClient() throws Exception {
            // Write HTTP response headers for SSE
            String headers = "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n";
            this.writeBuffer.put(headers.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            this.connectionMetadata.channel.write(this.writeBuffer);
            // need to set up read before adding this connection to sse client
            this.writeBuffer.clear();
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
            SSE_CLIENTS.add(this);
        }

        public void sendToSseClient(long numTIDsCommitted){
            String eventData = "data: " + numTIDsCommitted + "\n\n";
            this.writeBuffer.put(eventData.getBytes(StandardCharsets.UTF_8));
            this.writeBuffer.flip();
            try {
                this.connectionMetadata.channel.write(this.writeBuffer);
            } catch (Exception e) {
                System.out.println("Error caught: "+e.getMessage());
                SSE_CLIENTS.remove(this);
                // e.printStackTrace(System.out);
            }
            this.writeBuffer.clear();
        }

        private static final byte[] OK_RESPONSE_BYTES = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.UTF_8);

        private static final String UNKNOWN_ACCEPT = "Accept header value is not supported";

        private static final byte[] ERROR_RESPONSE_BYTES = ("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\nContent-Length: "+UNKNOWN_ACCEPT.length()+"\r\n\r\n"+UNKNOWN_ACCEPT).getBytes(StandardCharsets.UTF_8);

        @Override
        public void completed(Integer result, Integer attachment) {
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
            String request = StandardCharsets.UTF_8.decode(this.readBuffer).toString();
            this.process(request);
        }

        @Override
        public void failed(Throwable exc, Integer attachment) {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }
    }

    protected static boolean isHttpClient(String request) {
        String subStr = request.substring(0, Math.max(request.indexOf(' '), 0));
        switch (subStr){
            case "GET", "PATCH", "POST", "PUT" -> {
                return true;
            }
        }
        return false;
    }

}
