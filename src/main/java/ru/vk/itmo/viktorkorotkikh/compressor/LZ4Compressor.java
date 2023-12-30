package ru.vk.itmo.viktorkorotkikh.compressor;

import net.jpountz.lz4.LZ4Factory;

import java.io.IOException;

public class LZ4Compressor implements Compressor {
    public static LZ4Compressor INSTANCE = new LZ4Compressor();
    private final net.jpountz.lz4.LZ4Compressor lz4Compressor;

    public LZ4Compressor() {
        this.lz4Compressor = LZ4Factory.fastestInstance().fastCompressor();
    }

    @Override
    public byte[] compress(byte[] src) throws IOException {
        return lz4Compressor.compress(src);
    }

    @Override
    public byte[] compress(byte[] src, int len) throws IOException {
        return lz4Compressor.compress(src, 0, len);
    }
}
