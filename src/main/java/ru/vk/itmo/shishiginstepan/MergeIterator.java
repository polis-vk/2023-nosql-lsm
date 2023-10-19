package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final List<IteratorWrapper> iterators;

    private final Comparator<MemorySegment> keyComparator = (o1, o2) -> {
        var mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }
        byte b1 = o1.get(ValueLayout.JAVA_BYTE, mismatch);
        byte b2 = o2.get(ValueLayout.JAVA_BYTE, mismatch);
        return Byte.compare(b1, b2);
    };

    private static class IteratorWrapper implements Iterator<Entry<MemorySegment>> {
        private Entry<MemorySegment> prefetched;
        private final Iterator<Entry<MemorySegment>> iterator;

        public IteratorWrapper(Iterator<Entry<MemorySegment>> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return this.iterator.hasNext() || this.prefetched != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (this.prefetched == null) {
                return this.iterator.next();
            } else {
                var toReturn = this.prefetched;
                this.prefetched = null;
                return toReturn;
            }
        }

        public Entry<MemorySegment> peekNext() {
            if (this.prefetched == null) {
                this.prefetched = this.iterator.next();
            }
            return this.prefetched;
        }

        public void skip() {
            if (this.prefetched != null) {
                this.prefetched = null;
                return;
            }
            this.iterator.next();
        }
    }

    public MergeIterator(List<Iterator<Entry<MemorySegment>>> iterators) {
        // приоритет мержа будет определен порядком итераторов
        this.iterators = new ArrayList<IteratorWrapper>();
        iterators.forEach(iterator -> this.iterators.add(
                new IteratorWrapper(iterator)
        ));
    }

    @Override
    public boolean hasNext() {
        for (var iterator : iterators) {
            if (iterator.hasNext()) return true;
        }
        return false;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> nextEntry = null;
        for (var iterator : this.iterators) {
            if (iterator.hasNext()) {
                nextEntry = iterator.peekNext();
                break;
            }
        }
        if (nextEntry == null) {
            throw new EmptyIteratorAccessed();
        }
        for (var iterator : this.iterators) {
            if (!iterator.hasNext()) continue;
            if (keyComparator.compare(nextEntry.key(), iterator.peekNext().key()) > 0) {
                nextEntry = iterator.peekNext();
            }
        }
        // Пропуск всех значений с теми же ключами (записи которые "перетираются" более новыми)
        for (var iterator : this.iterators) {
            if (!iterator.hasNext()) continue;
            if (keyComparator.compare(nextEntry.key(), iterator.peekNext().key()) == 0) {
                iterator.skip();
            }
        }
        return nextEntry;
    }

    private static class EmptyIteratorAccessed extends RuntimeException {
    }
}
