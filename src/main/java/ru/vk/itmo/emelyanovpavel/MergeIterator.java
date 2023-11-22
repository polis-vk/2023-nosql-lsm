package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Queue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final Queue<PeekIterator<Entry<MemorySegment>>> priorityQueue;

    public MergeIterator(List<PeekIterator<Entry<MemorySegment>>> iterators) {
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                getPriorityQueueComparator()
        );
        List<PeekIterator<Entry<MemorySegment>>> iteratorsCopy = iterators.stream()
                .filter(Objects::nonNull)
                .filter(Iterator::hasNext)
                .toList();
        priorityQueue.addAll(iteratorsCopy);
        skipDeletedEntry();
    }

    @Override
    public boolean hasNext() {
        return !priorityQueue.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        PeekIterator<Entry<MemorySegment>> it = priorityQueue.remove();
        Entry<MemorySegment> current = it.next();
        deleteByKey(current.key());
        if (it.hasNext()) {
            priorityQueue.add(it);
        }
        skipDeletedEntry();
        return current;
    }

    private void deleteByKey(MemorySegment key) {
        while (!priorityQueue.isEmpty() && priorityQueue.peek().peek().key().mismatch(key) == -1) {
            PeekIterator<Entry<MemorySegment>> it = priorityQueue.remove();
            it.next();
            if (it.hasNext()) {
                priorityQueue.add(it);
            }
        }
    }

    private static Comparator<? super PeekIterator<Entry<MemorySegment>>> getPriorityQueueComparator() {
        return Comparator
                .comparing(
                        (PeekIterator<Entry<MemorySegment>> it) -> it.peek().key(),
                        new MemorySegmentComparator()
                )
                .thenComparing(
                        PeekIterator::getPriority,
                        Comparator.reverseOrder()
                );
    }

    private void skipDeletedEntry() {
        while (isCurrentElementEmpty()) {
            PeekIterator<Entry<MemorySegment>> it = priorityQueue.remove();
            deleteByKey(it.next().key());
            if (it.hasNext()) {
                priorityQueue.add(it);
            }
        }
    }

    private boolean isCurrentElementEmpty() {
        return !priorityQueue.isEmpty() && priorityQueue.peek().peek().value() == null;
    }
}
