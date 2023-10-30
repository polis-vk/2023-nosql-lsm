package ru.vk.itmo.mozzhevilovdanil.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import static ru.vk.itmo.mozzhevilovdanil.DatabaseUtils.comparator;

public class DatabaseIterator implements Iterator<Entry<MemorySegment>> {

    private static final Comparator<PeekIterator<Entry<MemorySegment>>> mergeIteratorComparatorWithoutId =
            (o1, o2) -> comparator.compare(o1.peek().key(), o2.peek().key());
    private static final Comparator<PeekIterator<Entry<MemorySegment>>> mergeIteratorComparator = (o1, o2) -> {
        int compare = mergeIteratorComparatorWithoutId.compare(o1, o2);
        if (compare == 0) {
            return Long.compare(o1.getId(), o2.getId());
        }
        return compare;
    };
    private final PriorityQueue<PeekIterator<Entry<MemorySegment>>> queue =
            new PriorityQueue<>(mergeIteratorComparator);
    private Entry<MemorySegment> allIteratorActualTop;

    public DatabaseIterator(Iterator<Entry<MemorySegment>> storageIterator,
                            List<Iterator<Entry<MemorySegment>>> iterators) {
        if (storageIterator.hasNext()) {
            queue.add(new PeekIterator<>(storageIterator, 0));
        }
        for (int i = 0; i < iterators.size(); i++) {
            if (iterators.get(i).hasNext()) {
                queue.add(new PeekIterator<>(iterators.get(i), (i + 1)));
            }
        }
    }

    @Override
    public boolean hasNext() {
        Entry<MemorySegment> topPeekValue;
        do {
            if (allIteratorActualTop != null) {
                return true;
            }
            if (queue.isEmpty()) {
                return false;
            }
            PeekIterator<Entry<MemorySegment>> topPeekIterator = queue.peek();
            queue.poll();

            deleteSamePeeksOnOtherIterators(topPeekIterator);

            topPeekValue = topPeekIterator.next();
            if (topPeekIterator.hasNext()) {
                queue.add(topPeekIterator);
            }

        } while (topPeekValue.value() == null);
        allIteratorActualTop = topPeekValue;
        return true;
    }

    private void deleteSamePeeksOnOtherIterators(PeekIterator<Entry<MemorySegment>> topPeekIterator) {
        while (queue.peek() != null) {
            PeekIterator<Entry<MemorySegment>> peek = queue.peek();
            if (mergeIteratorComparatorWithoutId.compare(topPeekIterator, peek) != 0) {
                break;
            }
            queue.poll();
            peek.next();
            if (peek.hasNext()) {
                queue.add(peek);
            }
        }
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            return null;
        }
        Entry<MemorySegment> result = allIteratorActualTop;
        allIteratorActualTop = null;
        return result;
    }
}
