package ru.vk.itmo.viktorkorotkikh.io.read;

import ru.vk.itmo.Entry;
import ru.vk.itmo.viktorkorotkikh.LSMPointerIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
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

    public Entry<MemorySegment> get(MemorySegment key) {
        try {
            long entryOffset = getEntryOffset(key, SearchOption.EQ);
            if (entryOffset == -1) {
                return null;
            }
            return getByIndex(entryOffset);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public abstract LSMPointerIterator iterator(MemorySegment from, MemorySegment to) throws Exception;

    protected abstract Entry<MemorySegment> getByIndex(long index) throws IOException;

    protected abstract long getEntryOffset(MemorySegment key, SearchOption searchOption) throws IOException;

    public boolean hasNoTombstones() {
        return mappedIndexFile.get(ValueLayout.JAVA_BOOLEAN, 0);
    }

    public enum SearchOption {
        EQ, GTE, LT
    }
}
