package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * A union iterator that contains a {@link PriorityQueue} with {@link PriorityPeekIterator}.
 *
 * <p>When the next element is retrieved, the keys from the two iterators are compared.
 * If the keys are not equal, then the {@link Entry} of the smaller key is returned,
 * otherwise the priorities of two iterators are compared and the one with the lower value is returned.
 */
public class MergePriorityPeekIterator implements PeekIterator<Entry<MemorySegment>> {

    private final PriorityQueue<PriorityPeekIterator<Entry<MemorySegment>>> priorityQueue;
    private PriorityPeekIterator<Entry<MemorySegment>> peekedIterator;

    public MergePriorityPeekIterator(List<PriorityPeekIterator<Entry<MemorySegment>>> iterators,
                                     PriorityPeekIteratorComparator priorityPeekIteratorComparator) {
        this.priorityQueue = new PriorityQueue<>(iterators.size(), priorityPeekIteratorComparator);

        for (PriorityPeekIterator<Entry<MemorySegment>> iterator : iterators) {
            if (iterator.hasNext()) {
                priorityQueue.add(iterator);
            }
        }
    }

    @Override
    public boolean hasNext() {
        return peekedIterator != null || !priorityQueue.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        PriorityPeekIterator<Entry<MemorySegment>> iterator;
        if (peekedIterator == null) {
            iterator = priorityQueue.remove();
        } else {
            iterator = peekedIterator;
        }

        Entry<MemorySegment> entry = iterator.next();
        while (!priorityQueue.isEmpty()) {
            PriorityPeekIterator<Entry<MemorySegment>> nextIterator = priorityQueue.peek();
            if (nextIterator.peek().key().mismatch(entry.key()) != -1) {
                break;
            }

            priorityQueue.remove();
            nextIterator.next();
            if (nextIterator.hasNext()) {
                priorityQueue.add(nextIterator);
            }
        }

        if (iterator.hasNext()) {
            priorityQueue.add(iterator);
        }
        peekedIterator = null;
        return entry;
    }

    @Override
    public Entry<MemorySegment> peek() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        if (peekedIterator == null) {
            peekedIterator = priorityQueue.remove();
        }
        return peekedIterator.peek();
    }
}
