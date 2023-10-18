package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;

public class SSTablePeekableIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegment ms;
    private long offset;
    private MemorySegment end;
    private Entry<MemorySegment> currentEntry;
    private long currentTimestamp;
    private boolean hasCurrent;

    public SSTablePeekableIterator(MemorySegment ms, long offset, MemorySegment end) {
        this.ms = ms;
        this.offset = offset;
        this.end = end;

        if (offset < ms.byteSize()) {
            readEntry();
            if (end != null && currentEntry.key().mismatch(end) == -1) {
                hasCurrent = false;
                return;
            }
            hasCurrent = true;
        } else {
            hasCurrent = false;
        }
    }

    public Entry<MemorySegment> getCurrentEntry() {
        return currentEntry;
    }

    public MemorySegment getCurrentKey() {
        return currentEntry.key();
    }

    public long getCurrentTimestamp() {
        return currentTimestamp;
    }

    @Override
    public boolean hasNext() {
        return hasCurrent;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = currentEntry;
        if (offset < ms.byteSize()) {
            readEntry();
            if (end != null && currentEntry.key().mismatch(end) == -1) {
                hasCurrent = false;
            }
        } else {
            hasCurrent = false;
        }
        return result;
    }

    private void readEntry() {
        long keySize = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        MemorySegment key = ms.asSlice(offset, keySize);
        offset += keySize;
        long valueSize = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        MemorySegment value = null;
        if (valueSize != -1) {
            value = ms.asSlice(offset, valueSize);
            offset += valueSize;
        }
        currentEntry = new BaseEntry<>(key, value);
        currentTimestamp = ms.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
    }
}
