package ru.vk.itmo.ershovvadim.hw3;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final Queue<PeekIterator<Entry<MemorySegment>>> priorityQueue;

    public MergeIterator(List<PeekIterator<Entry<MemorySegment>>> iterators) {
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                Comparator
                        .comparing((PeekIterator<Entry<MemorySegment>> iter) -> iter.peek().key(), Utils::compare)
                        .thenComparing(PeekIterator::getPriority, Comparator.reverseOrder())
        );
        for (PeekIterator<Entry<MemorySegment>> iterator : iterators) {
            if (iterator.hasNext()) {
                priorityQueue.add(iterator);
            }
        }
    }

    @Override
    public boolean hasNext() {
        skipDeletedEntry();
        return !priorityQueue.isEmpty();
    }

    private void skipDeletedEntry() {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().peek().value() == null) {
            PeekIterator<Entry<MemorySegment>> currentItr = priorityQueue.remove();
            Entry<MemorySegment> current = currentItr.next();
            removeOldEntryByKey(current.key());
            if (currentItr.hasNext()) {
                priorityQueue.add(currentItr);
            }
        }
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekIterator<Entry<MemorySegment>> currentItr = priorityQueue.remove();
        Entry<MemorySegment> current = currentItr.next();
        removeOldEntryByKey(current.key());
        if (currentItr.hasNext()) {
            priorityQueue.add(currentItr);
        }
        return current;
    }

    private void removeOldEntryByKey(MemorySegment key) {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().peek().key().mismatch(key) == -1) {
            PeekIterator<Entry<MemorySegment>> currentItr = priorityQueue.remove();
            currentItr.next();
            if (currentItr.hasNext()) {
                priorityQueue.add(currentItr);
            }
        }
    }

}
