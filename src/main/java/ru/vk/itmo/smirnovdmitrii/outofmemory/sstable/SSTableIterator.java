package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.outofmemory.RangeRequestGroup;
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
    private final RangeRequestGroup group;
    private final EqualsComparator<MemorySegment> comparator;
    private SSTable ssTable;
    private Entry<MemorySegment> next;
    private long upperBoundOffset;
    private long offset;

    public SSTableIterator(
            final SSTable ssTable,
            final RangeRequestGroup group,
            final MemorySegment from,
            final MemorySegment to,
            final SSTableStorage ssTableStorage,
            final EqualsComparator<MemorySegment> comparator
    ) {
        this.storage = ssTableStorage;
        this.group = group;
        this.comparator = comparator;
        this.upperBound = to;
        this.ssTable = ssTable;
        if (!group.register(ssTable)) {
            this.ssTable = null;
            this.next = null;
            return;
        }
        this.next = new BaseEntry<>(from, null);
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
            try (OpenedSSTable openedSStable = ssTable.open()) {
                if (openedSStable != null) {
                    next = openedSStable.readBlock(offset++);
                    return result;
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
            try (OpenedSSTable openedSSTable = ssTable.open()) {
                if (openedSSTable != null) {
                    long binarySearchResult = next.key() == null ? 0
                            : openedSSTable.binarySearch(next.key(), comparator);
                    upperBoundOffset = upperBound == null ? openedSSTable.blockCount()
                            : openedSSTable.upperBound(upperBound, comparator);
                    offset = SSTableUtil.normalize(binarySearchResult);
                    return binarySearchResult == offset;
                }
            }
            final SSTable compaction = storage.getCompaction(ssTable);
            if (compaction == null || !group.register(compaction)) {
                ssTable = null;
                next = null;
                return false;
            }
            ssTable = compaction;
        }
    }

}
