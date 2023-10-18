package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import java.lang.foreign.MemorySegment;

public class IdentityMemorySegmentSerializer implements MemorySegmentSerializer<MemorySegment, Entry<MemorySegment>> {
    public static final IdentityMemorySegmentSerializer INSTANCE = new IdentityMemorySegmentSerializer();

    @Override
    public MemorySegment toValue(MemorySegment input) {
        return input;
    }

    @Override
    public MemorySegment fromValue(MemorySegment value) {
        return value;
    }

    @Override
    public long size(MemorySegment value) {
        return value == null ? 0 : value.byteSize();
    }

    @Override
    public Entry<MemorySegment> createEntry(MemorySegment key, MemorySegment value) {
        return new BaseEntry<>(key, value);
    }
}
