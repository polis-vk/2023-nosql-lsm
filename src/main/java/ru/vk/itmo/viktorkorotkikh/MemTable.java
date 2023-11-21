package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTable {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    private final long flushThresholdBytes;

    private long memTableByteSize;

    private int memTableEntriesSize;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public MemTable(long flushThresholdBytes) {
        this.flushThresholdBytes = flushThresholdBytes;
        this.storage = createNewMemTable();
    }

    private static NavigableMap<MemorySegment, Entry<MemorySegment>> createNewMemTable() {
        return new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
    }

    private Iterator<Entry<MemorySegment>> storageIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.sequencedValues().iterator();
        }

        if (from == null) {
            return storage.headMap(to).sequencedValues().iterator();
        }

        if (to == null) {
            return storage.tailMap(from).sequencedValues().iterator();
        }

        return storage.subMap(from, to).sequencedValues().iterator();
    }

    public MemTableIterator iterator(MemorySegment from, MemorySegment to) {
        return new MemTableIterator(storageIterator(from, to));
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    public Collection<Entry<MemorySegment>> values() {
        return storage.values();
    }

    public boolean upsert(Entry<MemorySegment> entry) {
        readWriteLock.writeLock().lock();
        try {
            if (memTableByteSize >= flushThresholdBytes) {
                throw new LSMDaoOutOfMemoryException();
            }
            Entry<MemorySegment> previous = storage.put(entry.key(), entry);
            if (previous == null) {
                memTableEntriesSize += 1;
            } else { // entry already was in memTable, so we need to substructure subtract size of previous entry
                memTableByteSize -= Utils.getEntrySize(previous);
            }
            memTableByteSize += Utils.getEntrySize(entry);
            return memTableByteSize >= flushThresholdBytes;
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    public boolean isEmpty() {
        readWriteLock.readLock().lock();
        try {
            return memTableByteSize == 0;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public int getEntriesSize() {
        readWriteLock.readLock().lock();
        try {
            return memTableEntriesSize;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public long getByteSize() {
        readWriteLock.readLock().lock();
        try {
            return memTableByteSize;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    public static final class MemTableIterator extends LSMPointerIterator {
        private final Iterator<Entry<MemorySegment>> iterator;
        private Entry<MemorySegment> current;

        private MemTableIterator(Iterator<Entry<MemorySegment>> storageIterator) {
            this.iterator = storageIterator;
            if (iterator.hasNext()) {
                current = iterator.next();
            }
        }

        @Override
        int getPriority() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected MemorySegment getPointerKeySrc() {
            return current.key();
        }

        @Override
        protected long getPointerKeySrcOffset() {
            return 0;
        }

        @Override
        boolean isPointerOnTombstone() {
            return current.value() == null;
        }

        @Override
        void shift() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            current = iterator.hasNext() ? iterator.next() : null;
        }

        @Override
        long getPointerSize() {
            return Utils.getEntrySize(current);
        }

        @Override
        protected long getPointerKeySrcSize() {
            return current.key().byteSize();
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<MemorySegment> entry = current;
            current = iterator.hasNext() ? iterator.next() : null;
            return entry;
        }
    }
}
