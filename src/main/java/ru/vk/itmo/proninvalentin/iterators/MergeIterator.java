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
        saveActualEntryAndmoveNext();
//        System.out.println("Iterators:");
//        iterators.forEach(it -> System.out.println(it.getPriority() + ":" + printEntry(it.getCurrent())));
    }

//    private String printEntry(Entry<MemorySegment> entry) {
//        if(entry == null){
//            return "NULL";
//        }
//        return ("[" + new DaoFactoryImpl().toString(entry.key()) + ":" + new DaoFactoryImpl().toString(entry.value()) + "]");
//    }

    @Override
    public boolean hasNext() {
        return actualEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = actualEntry;
        saveActualEntryAndmoveNext();
        return result;
    }

    private void saveActualEntryAndmoveNext() {
        Entry<MemorySegment> result = null;

        while (result == null && !iterators.isEmpty()) {
            PeekingPriorityIterator iterator = iterators.poll();
            result = iterator.getCurrent();

            refreshIteratorsWithSameEntryKey(result);
            refreshIterator(iterator);

            result = result.value() == null ? null : result;
        }

        actualEntry = result;
//        System.out.println("Actual entry: " + printEntry(actualEntry));
    }

    private void refreshIteratorsWithSameEntryKey(Entry<MemorySegment> current) {
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
