package ru.vk.itmo.reshetnikovaleksei.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Queue;

public final class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final Queue<PeekingIterator> queue;

    private MergeIterator(Queue<PeekingIterator> queue) {
        this.queue = queue;
    }

    public static Iterator<Entry<MemorySegment>> merge(
            List<PeekingIterator> iterators, Comparator<MemorySegment> comparator) {
        if (iterators.isEmpty()) {
            return Collections.emptyIterator();
        }

        Queue<PeekingIterator> queue = new PriorityQueue<>(
                iterators.size(),
                Comparator.comparing((PeekingIterator iter) -> iter.peek().key(), comparator)
                        .thenComparing(PeekingIterator::priority, Comparator.reverseOrder())
        );

        for (PeekingIterator iterator : iterators) {
            if (iterator.hasNext()) {
                queue.add(iterator);
            }
        }

        return new MergeIterator(queue);
    }

    @Override
    public boolean hasNext() {
        skipDeletedEntry();
        return !queue.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("no next element");
        }

        PeekingIterator currIterator = queue.remove();
        Entry<MemorySegment> currEntry = currIterator.next();
        removeOldEntryByKey(currEntry.key());
        if (currIterator.hasNext()) {
            queue.add(currIterator);
        }

        return currEntry;
    }

    private void skipDeletedEntry() {
        while (!queue.isEmpty() && queue.peek().peek().value() == null) {
            PeekingIterator currentItr = queue.remove();
            Entry<MemorySegment> current = currentItr.next();
            removeOldEntryByKey(current.key());
            if (currentItr.hasNext()) {
                queue.add(currentItr);
            }
        }
    }

    private void removeOldEntryByKey(MemorySegment key) {
        while (!queue.isEmpty() && queue.peek().peek().key().mismatch(key) == -1) {
            PeekingIterator currentItr = queue.remove();
            currentItr.next();
            if (currentItr.hasNext()) {
                queue.add(currentItr);
            }
        }
    }
}
