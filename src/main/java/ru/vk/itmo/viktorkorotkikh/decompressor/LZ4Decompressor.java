package ru.vk.itmo.viktorkorotkikh.decompressor;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

import java.io.IOException;

public class LZ4Decompressor implements Decompressor {
    public static final LZ4Decompressor INSTANCE = new LZ4Decompressor();
    private final LZ4FastDecompressor lz4Decompressor;

    public LZ4Decompressor() {
        this.lz4Decompressor = LZ4Factory.fastestInstance().fastDecompressor();
    }

    @Override
    public void decompress(byte[] src, byte[] dest, int destOff, int uncompressedSize) throws IOException {
        lz4Decompressor.decompress(src, 0, dest, destOff, uncompressedSize);
    }
}
