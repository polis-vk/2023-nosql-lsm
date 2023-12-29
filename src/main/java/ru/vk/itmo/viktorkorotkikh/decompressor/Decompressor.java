package ru.vk.itmo.viktorkorotkikh.decompressor;

import java.io.IOException;

public interface Decompressor {

    void decompress(byte[] src, byte[] dest, int destOff, int uncompressedSize) throws IOException;
}
