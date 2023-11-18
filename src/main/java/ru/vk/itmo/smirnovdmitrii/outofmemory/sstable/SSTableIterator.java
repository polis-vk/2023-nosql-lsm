package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.EqualsComparator;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for SSTable.
 */
public class SSTableIterator implements Iterator<Entry<MemorySegment>> {
    private final MemorySegment upperBound;
    private final SSTableStorage storage;
    private final EqualsComparator<MemorySegment> comparator;
    private SSTable ssTable;
    private Entry<MemorySegment> next;
    private long upperBoundOffset;
    private long offset;

    public SSTableIterator(
            final SSTable ssTable,
            final MemorySegment from,
            final MemorySegment to,
            final SSTableStorage ssTableStorage,
            final EqualsComparator<MemorySegment> comparator
    ) {
        this.ssTable = ssTable;
        this.next = new BaseEntry<>(from, null);
        this.upperBound = to;
        this.storage = ssTableStorage;
        this.comparator = comparator;
        reposition();
        safeNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }
        return safeNext();
    }

    /**
     * Returns null if where is no next element.
     * @return next element.
     */
    private Entry<MemorySegment> safeNext() {
        final Entry<MemorySegment> result = next;
        while (true) {
            if (ssTable == null || offset == upperBoundOffset) {
                next = null;
                return result;
            }
            if (ssTable.tryOpen()) {
                try {
                    next = SSTableUtil.readBlock(ssTable, offset++);
                    return result;
                } finally {
                    ssTable.close();
                }
            }
            if (reposition()) {
                offset++;
            }
        }
    }

    /**
     * Tries to position current SSTable, if SSTable is dead then tries to find SSTable where to read from.
     * @return true if after repositioning current element equals to element before repositioning.
     */
    private boolean reposition() {
        while (true) {
            if (ssTable.tryOpen()) {
                try {
                    long binarySearchResult = next.key() == null ? 0
                            : SSTableUtil.binarySearch(next.key(), ssTable, comparator);
                    upperBoundOffset = upperBound == null ? SSTableUtil.blockCount(ssTable)
                            : SSTableUtil.upperBound(upperBound, ssTable, comparator);
                    offset = SSTableUtil.normalize(binarySearchResult);
                    return binarySearchResult == offset;
                } finally {
                    ssTable.close();
                }
            }
            ssTable = storage.getCompaction(ssTable);
        }
    }

}
