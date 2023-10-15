package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

/**
 * Decorator pattern.
 */
public class MemorySegmentCell extends Cell<MemorySegment> {
    private final Entry<MemorySegment> entry;

    public MemorySegmentCell(Entry<MemorySegment> entry) {
        this.entry = entry;
    }

    @Override
    public MemorySegment key() {
        return entry.key();
    }

    @Override
    public MemorySegment value() {
        return entry.value();
    }

    @Override
    public long valueSize() {
        return entry.value() == null ? 0 : entry.value().byteSize();
    }

    @Override
    public boolean isTombstone() {
        return (entry.value() == null);
    }

    public static class CellFactory implements Factory<MemorySegment> {

        @Override
        public MemorySegmentCell create(Entry<MemorySegment> entry) {
            return new MemorySegmentCell(entry);
        }

        @Override
        public MemorySegmentCell create(MemorySegment key, MemorySegment value) {
            return new MemorySegmentCell(new BaseEntry<>(key, value));
        }
    }
}