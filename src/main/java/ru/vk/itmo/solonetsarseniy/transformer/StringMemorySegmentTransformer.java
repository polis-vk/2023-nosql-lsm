package ru.vk.itmo.solonetsarseniy.transformer;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class StringMemorySegmentTransformer implements Transformer<String, MemorySegment> {
    private final Transformer<String, ByteBuffer> stringByteBufferTransformer =
        new StringByteBufferTransformer();

    private final Transformer<ByteBuffer, MemorySegment> byteBufferMemorySegmentTransformer =
        new ByteBufferMemorySegmentTransformer();

    @Override
    public String toTarget(MemorySegment source) {
        var byteBuffer = byteBufferMemorySegmentTransformer.toTarget(source);
        return stringByteBufferTransformer.toTarget(byteBuffer);
    }

    @Override
    public MemorySegment toSource(String target) {
        var byteBuffer = stringByteBufferTransformer.toSource(target);
        return byteBufferMemorySegmentTransformer.toSource(byteBuffer);
    }
}
