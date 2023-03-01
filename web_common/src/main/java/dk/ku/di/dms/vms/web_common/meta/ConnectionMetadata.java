package dk.ku.di.dms.vms.web_common.meta;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;

/**
 * Simple metadata about a connection
 */
public record ConnectionMetadata (
        ByteBuffer buffer,
        AsynchronousSocketChannel channel)
{}
