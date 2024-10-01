package dk.ku.di.dms.vms.modb.common.compressing;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.nio.ByteBuffer;

/**
 * A proxy to LZ4
 */
public final class CompressingUtils {

    private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
    private static final LZ4Compressor LZ4_COMPRESSOR = LZ4_FACTORY.fastCompressor();
    private static final LZ4FastDecompressor LZ4_DECOMPRESSOR = LZ4_FACTORY.fastDecompressor();

    private CompressingUtils(){}

    public static int maxCompressedLength(int size){
        return LZ4_COMPRESSOR.maxCompressedLength(size);
    }

    public static void compress(ByteBuffer sourceBuffer,  ByteBuffer targetBuffer){
        LZ4_COMPRESSOR.compress(sourceBuffer, targetBuffer);
    }

    public static void compress(ByteBuffer src, int srcOff, int srcLen, ByteBuffer dest, int destOff, int maxDestLen){
        LZ4_COMPRESSOR.compress(src, srcOff, srcLen, dest, destOff, maxDestLen);
    }

    public static void decompress(ByteBuffer sourceBuffer, ByteBuffer targetBuffer){
        LZ4_DECOMPRESSOR.decompress(sourceBuffer, targetBuffer);
    }

    public static void decompress(ByteBuffer src, int srcOff, ByteBuffer dest, int destOff, int destLen){
        LZ4_DECOMPRESSOR.decompress(src, srcOff, dest, destOff, destLen);
    }

}
