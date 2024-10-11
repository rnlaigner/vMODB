package dk.ku.di.dms.vms.web_common.channel;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.Pipe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class PipeChannel implements IChannel {

    // Get the sink and source channels from the pipe
    private final Pipe.SinkChannel sinkChannel;
    private final Pipe.SourceChannel sourceChannel;

    public static PipeChannel create(){
        try{
            var pipe = Pipe.open();
            return new PipeChannel(pipe);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public PipeChannel(Pipe pipe){
        this.sinkChannel = pipe.sink();
        this.sourceChannel = pipe.source();
    }

    @Override
    public void write(ByteBuffer src){
        try {
            this.sinkChannel.write(src);
        } catch (IOException ignored) { }
    }

    @Override
    public <A> void write(ByteBuffer src, long timeout, TimeUnit unit, A attachment, CompletionHandler<Integer, ? super A> handler) {
        try {
            int value = this.sinkChannel.write(src);
            handler.completed(value, attachment);
        } catch (IOException e) {
            handler.failed(e, attachment);
        }
    }

    @Override
    public <A> void read(ByteBuffer dst, A attachment, CompletionHandler<Integer, ? super A> handler) {
        try {
            int res = this.sourceChannel.read(dst);
            handler.completed(res, attachment);
        } catch (Exception e){
            handler.failed(e, attachment);
        }
    }

    @Override
    public Future<Integer> read(ByteBuffer dst) {
        try {
            return CompletableFuture.completedFuture(this.sourceChannel.read(dst));
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public void close(){
        try {
            this.sourceChannel.close();
            this.sinkChannel.close();
        } catch (Exception ignored){}
    }

    @Override
    public boolean isOpen() {
        return this.sourceChannel.isOpen() || this.sinkChannel.isOpen();
    }

}
