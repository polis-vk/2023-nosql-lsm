package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class PriorityIterator implements Iterator<Entry<MemorySegment>> {

    private final PriorityQueue<IndexedPeekIterator> priorityQueue;

    public PriorityIterator(Collection<IndexedPeekIterator> iterators) {
        if (iterators.isEmpty()) {
            priorityQueue = new PriorityQueue<>();
        } else {
            priorityQueue = new PriorityQueue<>(
                    iterators.size(),
                    Comparator
                            .comparing(
                                    (IndexedPeekIterator it) -> it.peek().key(),
                                    MemorySegmentUtils::compare)
                            .thenComparingLong(IndexedPeekIterator::index)
            );
            priorityQueue.addAll(iterators);
        }
    }

    @Override
    public boolean hasNext() {
        skipTombstone();
        return !priorityQueue.isEmpty();
    }


    /**
     * Tombstone - entry with value = null | valueOffset = -1;
     */
    private void skipTombstone() {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().peek().value() == null) {
            IndexedPeekIterator currentIterator = priorityQueue.remove();
            Entry<MemorySegment> entry = currentIterator.next();
            deleteFromOld(entry.key());
            if (currentIterator.hasNext()) {
                priorityQueue.add(currentIterator);
            }
        }
    }

    @Override
    public Entry<MemorySegment> next() {
        if(!hasNext()) {
            throw new NoSuchElementException();
        }

        IndexedPeekIterator iterator = priorityQueue.remove();
        Entry<MemorySegment> nextEntry = iterator.next();

        deleteFromOld(nextEntry.key());
        if (iterator.hasNext()) {
            priorityQueue.add(iterator);
        }

        return nextEntry;
    }


    /**
     * @param key
     * Move iterators with equal key
     */
    private void deleteFromOld(MemorySegment key) {
        while (!priorityQueue.isEmpty()) {
            IndexedPeekIterator peekIterator = priorityQueue.peek();
            if (key.mismatch(peekIterator.peek().key()) != -1) {
                return;
            }
            priorityQueue.remove();
            peekIterator.next();
            if (peekIterator.hasNext()) {
                priorityQueue.add(peekIterator);
            }
        }
    }
}
