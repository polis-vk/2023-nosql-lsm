package ru.vk.itmo.viktorkorotkikh.compressor;

import java.io.IOException;

public interface Compressor {
    byte[] compress(byte[] src) throws IOException;

    byte[] compress(byte[] src, int len) throws IOException;
}
