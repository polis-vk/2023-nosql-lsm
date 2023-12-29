package ru.vk.itmo.viktorkorotkikh.io.read;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public abstract class AbstractSSTableReader {
    protected final MemorySegment mappedSSTable;
    protected final MemorySegment mappedIndexFile;
    protected final MemorySegment mappedCompressionInfo;
    protected final int index;

    protected AbstractSSTableReader(
            MemorySegment mappedSSTable,
            MemorySegment mappedIndexFile,
            MemorySegment mappedCompressionInfo,
            int index
    ) {
        this.mappedSSTable = mappedSSTable;
        this.mappedIndexFile = mappedIndexFile;
        this.mappedCompressionInfo = mappedCompressionInfo;
        this.index = index;
    }

    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        long entryOffset = getEntryOffset(key, SearchOption.EQ);
        if (entryOffset == -1) {
            return null;
        }
        return getByIndex(entryOffset);
    }

    public abstract Iterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) throws Exception;

    protected abstract Entry<MemorySegment> getByIndex(long index) throws IOException;

    protected abstract long getEntryOffset(MemorySegment key, SearchOption searchOption) throws IOException;

    public enum SearchOption {
        EQ, GTE, LT
    }
}
