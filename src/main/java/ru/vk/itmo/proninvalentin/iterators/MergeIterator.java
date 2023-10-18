package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.comparators.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    PriorityQueue<PeekingPriorityIterator> iterators;
    Entry<MemorySegment> actualEntry;
    MemorySegmentComparator msComparator = MemorySegmentComparator.getInstance();

    public MergeIterator(PeekingPriorityIterator inMemoryIterator,
                         List<PeekingPriorityIterator> inFileIterators) {
        iterators = new PriorityQueue<>();
        if (inMemoryIterator.hasNext()) {
            inMemoryIterator.next();
        }
        if (inMemoryIterator.getCurrent() != null) {
            iterators.add(inMemoryIterator);
        }
        var nonEmptyFileIterators = inFileIterators.stream().filter(Iterator::hasNext).toList();
        nonEmptyFileIterators.forEach(Iterator::next);
        iterators.addAll(inFileIterators.stream().filter(x -> x.getCurrent() != null).toList());
        moveNextAndSaveActualEntry();
    }

    @Override
    public boolean hasNext() {
        return actualEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = actualEntry;
        moveNextAndSaveActualEntry();
        return result;
    }

    private void moveNextAndSaveActualEntry() {
        Entry<MemorySegment> result = null;

        while (result == null && !iterators.isEmpty()) {
            PeekingPriorityIterator iterator = iterators.poll();
            result = iterator.getCurrent();

            addIteratorsWithSameKeyToHeap(result);
            refreshIterator(iterator);

            result = result.value() == null ? null : result;
        }

        actualEntry = result;
    }

    private void addIteratorsWithSameKeyToHeap(Entry<MemorySegment> current) {
        while (!iterators.isEmpty() && msComparator.compare(iterators.peek().getCurrent().key(), current.key()) == 0) {
            refreshIterator(iterators.remove());
        }
    }

    private void refreshIterator(PeekingPriorityIterator iterator) {
        if (iterator.hasNext()) {
            iterator.next();
            iterators.add(iterator);
        }
    }
}
