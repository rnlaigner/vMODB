package dk.ku.di.dms.vms.tpcc.proxy.infra;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import jdk.net.ExtendedSocketOptions;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

public final class MinimalHttpClient implements Closeable {

    private final SocketChannel socketChannel;
    private final ByteBuffer writeBuffer;
    private final ByteBuffer readBuffer;

    private final StringBuilder response;

    public MinimalHttpClient(String hostname, int port) throws IOException {
        this.socketChannel = SocketChannel.open(new InetSocketAddress(hostname, port));
        this.socketChannel.configureBlocking(true);
        this.socketChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
        this.socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        if(this.socketChannel.supportedOptions().contains(ExtendedSocketOptions.TCP_QUICKACK)) {
            this.socketChannel.setOption(ExtendedSocketOptions.TCP_QUICKACK, true);
        }
        this.writeBuffer = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        this.readBuffer = MemoryManager.getTemporaryDirectBuffer(MemoryUtils.DEFAULT_PAGE_SIZE);
        this.response = new StringBuilder();
    }

    private static String buildHttpRequest(String method, String payload, int length, String param){
        return method+" /"+param+" HTTP/1.1\r\n"
                + "Host: proxy-http\r\n"
                + "Content-Type: application/json\r\n"
                + "Content-Length: " + length + "\r\n\r\n"
                + payload; // + "\r\n"; // not necessary if following the req/rep http protocol
    }

    private static String buildHttpGetRequest(String param){
        return "GET /"+param+" HTTP/1.1\r\n"
                + "Host: proxy-http\r\n"
                + "Accept: application/json\r\n\r\n";
    }

    public String sendGetRequest(String param) throws IOException {
        String httpRequest = buildHttpGetRequest(param);
        this.writeBuffer.put(httpRequest.getBytes(StandardCharsets.UTF_8));
        this.writeBuffer.flip();
        do {
            this.socketChannel.write(this.writeBuffer);
        } while (this.writeBuffer.hasRemaining());
        this.writeBuffer.clear();

        this.response.setLength(0);

        // read to block waiting for response and avoid concatenating requests
        this.socketChannel.read(this.readBuffer);
        this.readBuffer.flip();
        while (this.readBuffer.hasRemaining()) {
            response.append((char) readBuffer.get());
        }
        this.readBuffer.clear();
        return response.toString();
    }

    public int sendRequest(String method, String jsonBody, String param) throws IOException {
        String httpRequest = buildHttpRequest(method, jsonBody, jsonBody.getBytes(StandardCharsets.UTF_8).length, param);
        this.writeBuffer.put(httpRequest.getBytes(StandardCharsets.UTF_8));
        this.writeBuffer.flip();
        do {
            this.socketChannel.write(this.writeBuffer);
        } while (this.writeBuffer.hasRemaining());
        this.writeBuffer.clear();
        // must read everything to avoid errors
        return this.readFully();
    }

    @Override
    public void close() {
        try { this.socketChannel.close(); } catch (IOException ignored) {}
        this.readBuffer.clear();
        this.writeBuffer.clear();
        MemoryManager.releaseTemporaryDirectBuffer(this.readBuffer);
        MemoryManager.releaseTemporaryDirectBuffer(this.writeBuffer);
    }

    private int readFully() throws IOException {
        StringBuilder responseBuilder = new StringBuilder();

        // headers
        boolean headersEnded = false;
        while (!headersEnded && socketChannel.read(this.readBuffer) > 0) {
            this.readBuffer.flip();
            byte[] bytes = new byte[this.readBuffer.remaining()];
            this.readBuffer.get(bytes);
            String chunk = new String(bytes, StandardCharsets.UTF_8);
            responseBuilder.append(chunk);
            this.readBuffer.clear();
            if (responseBuilder.toString().contains("\r\n\r\n")) {
                headersEnded = true;
            }
        }

        String headersPart = responseBuilder.toString();
        int status = Integer.parseInt(headersPart.split("\r\n", 2)[0].split(" ")[1]);

        // content-Length
        int contentLength = 0;
        for (String line : headersPart.split("\r\n")) {
            if (line.toLowerCase().startsWith("content-length:")) {
                contentLength = Integer.parseInt(line.split(":")[1].trim());
                break;
            }
        }

        // body
        int remaining = contentLength;
        while (remaining > 0) {
            int read = this.socketChannel.read(this.readBuffer);
            if (read == -1) break;  // end of stream
            this.readBuffer.flip();
            byte[] bytes = new byte[this.readBuffer.remaining()];
            this.readBuffer.get(bytes);
            remaining -= bytes.length;
            this.readBuffer.clear();
        }
        return status;
    }

}
