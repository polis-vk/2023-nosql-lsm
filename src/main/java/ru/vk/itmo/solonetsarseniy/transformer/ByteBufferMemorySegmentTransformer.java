package ru.vk.itmo.solonetsarseniy.transformer;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class ByteBufferMemorySegmentTransformer implements Transformer<ByteBuffer, MemorySegment> {
    @Override
    public ByteBuffer toTarget(MemorySegment source) {
        return source.asByteBuffer();
    }

    @Override
    public MemorySegment toSource(ByteBuffer target) {
        return MemorySegment.ofBuffer(target);
    }
}
