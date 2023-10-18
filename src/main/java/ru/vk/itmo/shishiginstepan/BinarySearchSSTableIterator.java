package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

class BinarySearchSSTableIterator implements Iterator<Entry<MemorySegment>> {
    private final long endEntryIndex;
    private long currentEntryIndex;

    private final MemorySegment indexSegment;
    private final MemorySegment tableSegment;

    public BinarySearchSSTableIterator(
            MemorySegment indexSegment,
            MemorySegment tableSegment,
            long startEntryIndex,
            long endEntryIndex
    ) {
        this.currentEntryIndex = startEntryIndex;
        this.endEntryIndex = endEntryIndex;
        this.indexSegment = indexSegment;
        this.tableSegment = tableSegment;
    }

    @Override
    public boolean hasNext() {
        return this.currentEntryIndex != this.endEntryIndex;
    }

    @Override
    public Entry<MemorySegment> next() {
        var keyOffset = this.indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, this.currentEntryIndex * Long.BYTES * 2);
        var valOffset = this.indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, this.currentEntryIndex * Long.BYTES * 2 + Long.BYTES);//TODO заменить по коменту из ПР
        long nextOffset;
        if (this.currentEntryIndex * Long.BYTES * 2 + Long.BYTES * 2 >= this.indexSegment.byteSize()) {
            nextOffset = this.tableSegment.byteSize();
        } else {
            nextOffset = this.indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, this.currentEntryIndex * Long.BYTES * 2 + Long.BYTES * 2);
        }
        this.currentEntryIndex++;
        return new BaseEntry<>(
                this.tableSegment.asSlice(keyOffset, valOffset - keyOffset),
                this.tableSegment.asSlice(valOffset, nextOffset - valOffset)
        );
    }
}
