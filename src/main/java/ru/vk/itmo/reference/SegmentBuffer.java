package ru.vk.itmo.reference;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Growable buffer with {@link ByteBuffer} and {@link MemorySegment} interface.
 *
 * @author incubos
 */
final class SegmentBuffer {
    private ByteBuffer blobBuffer;
    private MemorySegment blobSegment;

    SegmentBuffer(final int capacity) {
        this.blobBuffer = ByteBuffer.allocate(capacity);
        this.blobSegment = MemorySegment.ofBuffer(blobBuffer);
    }

    ByteBuffer buffer() {
        return blobBuffer.rewind();
    }

    MemorySegment segment() {
        return blobSegment;
    }

    void limit(final long limit) {
        if (limit > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too big!");
        }

        final int capacity = (int) limit;
        if (blobBuffer.capacity() >= capacity) {
            blobBuffer.limit(capacity);
            return;
        }

        // Grow to the nearest bigger power of 2
        final int size = Integer.highestOneBit(capacity) << 1;
        blobBuffer = ByteBuffer.allocate(size);
        blobSegment = MemorySegment.ofBuffer(blobBuffer);
    }
}
