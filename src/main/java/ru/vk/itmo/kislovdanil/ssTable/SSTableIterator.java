package ru.vk.itmo.kislovdanil.ssTable;

import ru.vk.itmo.Entry;
import ru.vk.itmo.kislovdanil.iterators.DatabaseIterator;

import java.lang.foreign.MemorySegment;

class SSTableIterator implements DatabaseIterator {
    private long curItemIndex;
    private final MemorySegment maxKey;

    private Entry<MemorySegment> curEntry;
    private SSTable table;

    public SSTableIterator(MemorySegment minKey, MemorySegment maxKey, SSTable table) {
        this.table = table;
        this.maxKey = maxKey;
        if (table.size == 0) return;
        this.table.readWriteLock.readLock().lock();
        try {
            if (minKey == null) {
                this.curItemIndex = 0;
            } else {
                this.curItemIndex = this.table.findByKey(minKey);
            }
            if (curItemIndex == -1) {
                curItemIndex = Long.MAX_VALUE;
            } else {
                this.curEntry = this.table.readEntry(curItemIndex);
            }
        } finally {
            this.table.readWriteLock.readLock().unlock();
        }
    }

    private void updateTable() {
        if (this.table.compactedTo == null) return;
        while (this.table.compactedTo != null) {
            this.table = this.table.compactedTo;
        }
        this.table.readWriteLock.readLock().lock();
        try {
            this.curItemIndex = this.table.findByKey(curEntry.key());
        } finally {
            this.table.readWriteLock.readLock().unlock();
        }
    }

    @Override
    public boolean hasNext() {
        updateTable();
        if (curItemIndex >= this.table.size) return false;
        return maxKey == null || table.memSegComp.compare(curEntry.key(), maxKey) < 0;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = curEntry;
        updateTable();
        this.table.readWriteLock.readLock().lock();
        try {
            curItemIndex++;
            if (curItemIndex < table.size) {
                curEntry = table.readEntry(curItemIndex);
            }
            return result;
        } finally {
            this.table.readWriteLock.readLock().unlock();
        }
    }

    @Override
    public long getPriority() {
        return table.getTableId();
    }
}
