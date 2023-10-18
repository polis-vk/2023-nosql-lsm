package ru.vk.itmo.kovalchukvladislav.model;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class DaoIterator<D, E extends Entry<D>> implements Iterator<E> {
    private static final ValueLayout.OfLong LONG_LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final Integer IN_MEMORY_ITERATOR_ID = Integer.MAX_VALUE;
    private final Iterator<E> inMemoryIterator;
    private final EntryExtractor<D, E> extractor;
    private final PriorityQueue<IndexedEntry> queue;
    private final List<StorageIterator> storageIterators;

    public DaoIterator(D from, D to,
                       Iterator<E> inMemoryIterator,
                       List<MemorySegment> storageSegments,
                       List<MemorySegment> offsetsSegments,
                       EntryExtractor<D, E> extractor) {
        this.extractor = extractor;
        this.inMemoryIterator = inMemoryIterator;
        this.storageIterators = getStorageIterators(from, to, storageSegments, offsetsSegments);
        this.queue = new PriorityQueue<>(1 + storageIterators.size());

        addEntryByIteratorIdSafe(IN_MEMORY_ITERATOR_ID);
        for (int i = 0; i < storageIterators.size(); i++) {
            addEntryByIteratorIdSafe(i);
        }
        cleanByNull();
    }

    private List<StorageIterator> getStorageIterators(D from, D to,
                                                      List<MemorySegment> storageSegments,
                                                      List<MemorySegment> offsetsSegments) {
        int storagesCount = storageSegments.size();
        final List<StorageIterator> iterators = new ArrayList<>(storagesCount);
        for (int i = 0; i < storagesCount; i++) {
            iterators.add(new StorageIterator(storageSegments.get(i), offsetsSegments.get(i), from, to));
        }
        return iterators;
    }

    @Override
    public boolean hasNext() {
        return !queue.isEmpty();
    }

    @Override
    public E next() {
        if (queue.isEmpty()) {
            throw new NoSuchElementException();
        }
        IndexedEntry minElement = queue.peek();
        E minEntry = minElement.entry;
        cleanByKey(minElement.entry.key());
        cleanByNull();
        return minEntry;
    }

    private void cleanByKey(D key) {
        while (!queue.isEmpty() && extractor.compare(queue.peek().entry.key(), key) == 0) {
            IndexedEntry removedEntry = queue.remove();
            int iteratorId = removedEntry.iteratorId;
            addEntryByIteratorIdSafe(iteratorId);
        }
    }

    private void cleanByNull() {
        while (!queue.isEmpty()) {
            E entry = queue.peek().entry;
            if (entry.value() != null) {
                break;
            }
            cleanByKey(entry.key());
        }
    }

    private void addEntryByIteratorIdSafe(int iteratorId) {
        Iterator<E> iteratorById = getIteratorById(iteratorId);
        if (iteratorById.hasNext()) {
            E next = iteratorById.next();
            queue.add(new IndexedEntry(iteratorId, next));
        }
    }

    private Iterator<E> getIteratorById(int id) {
        if (id == IN_MEMORY_ITERATOR_ID) {
            return inMemoryIterator;
        }
        return storageIterators.get(id);
    }

    private class IndexedEntry implements Comparable<IndexedEntry> {
        final int iteratorId;
        final E entry;

        public IndexedEntry(int iteratorId, E entry) {
            this.iteratorId = iteratorId;
            this.entry = entry;
        }

        @Override
        public int compareTo(IndexedEntry other) {
            int compared = extractor.compare(entry.key(), other.entry.key());
            if (compared != 0) {
                return compared;
            }
            return -Integer.compare(iteratorId, other.iteratorId);
        }
    }

    private class StorageIterator implements Iterator<E> {
        private final MemorySegment storageSegment;
        private final long end;
        private long start;

        public StorageIterator(MemorySegment storageSegment, MemorySegment offsetsSegment, D from, D to) {
            this.storageSegment = storageSegment;

            if (offsetsSegment.byteSize() == 0) {
                this.start = -1;
                this.end = -1;
            } else {
                this.start = calculateStartPosition(offsetsSegment, from);
                this.end = calculateEndPosition(offsetsSegment, to);
            }
        }

        private long calculateStartPosition(MemorySegment offsetsSegment, D from) {
            if (from == null) {
                return getFirstOffset(offsetsSegment);
            }
            long lowerBoundOffset = extractor.findLowerBoundValueOffset(from, storageSegment, offsetsSegment);
            if (lowerBoundOffset == -1) {
                // the smallest element and doesn't exist
                return getFirstOffset(offsetsSegment);
            } else {
                // storage[lowerBoundOffset] <= from, we need >= only
                return moveOffsetIfFirstKeyAreNotEqual(from, lowerBoundOffset);
            }
        }

        private long calculateEndPosition(MemorySegment offsetsSegment, D to) {
            if (to == null) {
                return getEndOffset();
            }
            long lowerBoundOffset = extractor.findLowerBoundValueOffset(to, storageSegment, offsetsSegment);
            if (lowerBoundOffset == -1) {
                // the smallest element and doesn't exist
                return getFirstOffset(offsetsSegment);
            }
            // storage[lowerBoundOffset] <= to, we need >= only
            return moveOffsetIfFirstKeyAreNotEqual(to, lowerBoundOffset);
        }

        private long getFirstOffset(MemorySegment offsetsSegment) {
            return offsetsSegment.getAtIndex(LONG_LAYOUT, 0);
        }

        private long getEndOffset() {
            return storageSegment.byteSize();
        }

        private long moveOffsetIfFirstKeyAreNotEqual(D from, long lowerBoundOffset) {
            long offset = lowerBoundOffset;
            D lowerBoundKey = extractor.readValue(storageSegment, offset);
            if (extractor.compare(lowerBoundKey, from) != 0) {
                offset += extractor.size(lowerBoundKey);
                D lowerBoundValue = extractor.readValue(storageSegment, offset);
                offset += extractor.size(lowerBoundValue);
            }
            return offset;
        }

        @Override
        public boolean hasNext() {
            return start < end;
        }

        @Override
        public E next() {
            E entry = extractor.readEntry(storageSegment, start);
            start += extractor.size(entry);
            return entry;
        }
    }
}
