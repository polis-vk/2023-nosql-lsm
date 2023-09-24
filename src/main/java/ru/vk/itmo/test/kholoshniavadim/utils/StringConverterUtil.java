package ru.vk.itmo.test.kholoshniavadim.utils;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class StringConverterUtil {
    private static final Charset charset = StandardCharsets.UTF_8;

    private StringConverterUtil() {
    }

    public static String toString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }

        final ByteBuffer byteBuffer = memorySegment.asByteBuffer();
        final byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return new String(bytes, charset);
    }

    public static MemorySegment fromString(String string) {
        if (string == null) {
            return null;
        }

        final byte[] bytes = string.getBytes(charset);
        return MemorySegment.ofArray(bytes);
    }
}
