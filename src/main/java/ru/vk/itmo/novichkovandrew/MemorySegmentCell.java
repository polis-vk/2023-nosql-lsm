package ru.vk.itmo.novichkovandrew;

import java.lang.foreign.MemorySegment;

public class MemorySegmentCell extends Cell<MemorySegment> {
    private final MemorySegment key;
    private final MemorySegment value;
    private final boolean tombstone;

    public MemorySegmentCell(MemorySegment key, MemorySegment value) {
        this.key = key;
        this.value = value;
        this.tombstone = (value == null);
    }


    @Override
    public MemorySegment key() {
        return this.key;
    }

    @Override
    public MemorySegment value() {
        return this.value;
    }

    @Override
    public long valueSize() {
        return value == null ? 0 : value.byteSize();
    }

    @Override
    public boolean isTombstone() {
        return this.tombstone;
    }
}
