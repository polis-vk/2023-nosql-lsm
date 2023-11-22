package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    private final PriorityQueue<PeekIterator> priorityQueue;
    private final Comparator<Entry<MemorySegment>> comparator;
    private static class PeekIterator implements Iterator<Entry<MemorySegment>> {

        public final int id;
        private final SegmentIterInterface delegate;
        private Entry<MemorySegment> peek;

        private PeekIterator(int id, SegmentIterInterface delegate) {
            this.id = id;
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            if (peek == null) {
                return delegate.hasNext();
            }
            return true;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<MemorySegment> peek = peek();
            this.peek = null;
            return peek;
        }

        private Entry<MemorySegment> peek() {
            if (peek == null) {
                if (!delegate.hasNext()) {
                    return null;
                }
                peek = delegate.next();
            }
            return peek;
        }
    }

    PeekIterator nextIterator;

    public MergeIterator(Collection<SegmentIterInterface> iterators, Comparator<Entry<MemorySegment>> comparator) {
        this.comparator = comparator;
        Comparator<PeekIterator> peekComp = (o1, o2) -> comparator.compare(o1.peek(), o2.peek());
        priorityQueue = new PriorityQueue<>(
                iterators.size(),
                peekComp.thenComparing(o -> -o.id)
        );

        int id = 0;
        for (SegmentIterInterface iterator : iterators) {
            if (iterator.hasNext()) {
                priorityQueue.add(new PeekIterator(id++, iterator));
            }
        }
    }

    private PeekIterator peek() {
        while (nextIterator == null) {
            nextIterator = priorityQueue.poll();
            if (nextIterator == null) {
                return null;
            }

            skipIteratorsWithSameKey();

            if (nextIterator.peek() == null) {
                nextIterator = null;
                continue;
            }

            if (shouldSkip(nextIterator.peek())) {
                moveNextAndPutBack(nextIterator);
                nextIterator = null;
            }
        }

        return nextIterator;
    }

    private void skipIteratorsWithSameKey() {
        while (true) {
            PeekIterator next = priorityQueue.peek();
            if (next == null) {
                break;
            }

            if (!skipTheSameKey(next)) {
                break;
            }
        }
    }

    private boolean skipTheSameKey(PeekIterator next) {
        int compare = comparator.compare(nextIterator.peek(), next.peek());
        if (compare != 0) {
            return false;
        }

        PeekIterator poll = priorityQueue.poll();
        if (poll != null) {
            moveNextAndPutBack(poll);
        }
        return true;
    }

    private void moveNextAndPutBack(PeekIterator poll) {
        poll.next();
        if (poll.hasNext()) {
            priorityQueue.add(poll);
        }
    }

    protected boolean shouldSkip(Entry<MemorySegment> t) {
        return false;
    }

    @Override
    public boolean hasNext() {
        return peek() != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        PeekIterator nextIterator = peek();
        if (nextIterator == null) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> nextValue = nextIterator.next();
        this.nextIterator = null;
        if (nextIterator.hasNext()) {
            priorityQueue.add(nextIterator);
        }
        return nextValue;
    }
}
