package ru.vk.itmo.solonetsarseniy.helpers;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;

public class MemorySegmentComparator implements Comparator<MemorySegment> {
    @Override
    public int compare(MemorySegment first, MemorySegment second) {
        ByteBuffer byteBufferFirst = first.asByteBuffer();
        byte[] byteArrayFirst = new byte[byteBufferFirst.remaining()];

        byteBufferFirst.get(byteArrayFirst);

        String strFirst = new String(byteArrayFirst, StandardCharsets.UTF_8);
        ByteBuffer byteBufferSecond = second.asByteBuffer();
        byte[] byteArraySecond = new byte[byteBufferSecond.remaining()];

        byteBufferSecond.get(byteArraySecond);

        String strSecond = new String(byteArraySecond, StandardCharsets.UTF_8);

        return strFirst.compareTo(strSecond);
    }
}
