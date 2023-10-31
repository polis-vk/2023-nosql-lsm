package ru.vk.itmo.supriadkinadaria;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final PriorityQueue<PeekIterator> iteratorPriorityQueue;
    private final Comparator<Entry<MemorySegment>> comparator;
    private PeekIterator peekIterator;

    public MergeIterator(Collection<Iterator<Entry<MemorySegment>>> iterators,
                         Comparator<Entry<MemorySegment>> comparator) {
        this.comparator = comparator;
        Comparator<PeekIterator> peekComp = (o1, o2) -> comparator.compare(o1.peek(), o2.peek());
        iteratorPriorityQueue = new PriorityQueue<>(
                iterators.size(),
                peekComp.thenComparing(o -> -o.id)
        );

        int id = 0;
        for (Iterator<Entry<MemorySegment>> iterator : iterators) {
            if (iterator.hasNext()) {
                iteratorPriorityQueue.add(new PeekIterator(id++, iterator));
            }
        }
    }

    private PeekIterator peek() {
        while (peekIterator == null) {
            peekIterator = iteratorPriorityQueue.poll();
            if (peekIterator == null) {
                return null;
            }

            filterIterators(iteratorPriorityQueue);

            if (peekIterator.peek() != null
                    && skip(peekIterator.peek())
                    && peekIterator.next() != null) {
                if (peekIterator.hasNext()) {
                    iteratorPriorityQueue.add(peekIterator);
                }
                peekIterator = null;
            }
        }

        return peekIterator;
    }

    private void filterIterators(PriorityQueue<PeekIterator> priorityQueue) {
        PeekIterator next = priorityQueue.peek();
        while (next != null) {
            int compare = comparator.compare(peekIterator.peek(), next.peek());
            if (compare == 0) {
                PeekIterator poll = priorityQueue.poll();
                if (poll != null
                        && poll.next() != null
                        && poll.hasNext()) {
                    priorityQueue.add(poll);
                }
            } else {
                break;
            }
            next = priorityQueue.peek();
        }
    }

    protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
        return memorySegmentEntry.value() == null;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        PeekIterator peekIteratorLocal = peek();
        if (peekIteratorLocal == null) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> next = peekIteratorLocal.next();
        this.peekIterator = null;
        if (peekIteratorLocal.hasNext()) {
            iteratorPriorityQueue.add(peekIteratorLocal);
        }
        return next;
    }

    private static class PeekIterator implements Iterator<Entry<MemorySegment>> {

        public final int id;
        private final Iterator<Entry<MemorySegment>> delegate;
        private Entry<MemorySegment> peekTmp;

        private PeekIterator(int id, Iterator<Entry<MemorySegment>> delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (peekTmp == null) {
                return delegate.hasNext();
            }
            return true;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<MemorySegment> peekNext = peek();
            this.peekTmp = null;
            return peekNext;
        }

        private Entry<MemorySegment> peek() {
            if (peekTmp == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                peekTmp = delegate.next();
            }
            return peekTmp;
        }
    }
}
