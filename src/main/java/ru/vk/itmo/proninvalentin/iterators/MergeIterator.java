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
        tryAddMemoryIterator(inMemoryIterator);
        tryAddFileIterators(inFileIterators);
        saveActualEntryAndMoveNext();
    }

    private void tryAddFileIterators(List<PeekingPriorityIterator> inFileIterators) {
        List<PeekingPriorityIterator> nonEmptyFileIterators = inFileIterators.stream()
                .filter(Iterator::hasNext)
                .toList();
        nonEmptyFileIterators.forEach(Iterator::next);
        iterators.addAll(inFileIterators.stream().filter(x -> x.getCurrent() != null).toList());
    }

    private void tryAddMemoryIterator(PeekingPriorityIterator inMemoryIterator) {
        if (inMemoryIterator.hasNext()) {
            inMemoryIterator.next();
        }
        if (inMemoryIterator.getCurrent() != null) {
            iterators.add(inMemoryIterator);
        }
    }

    @Override
    public boolean hasNext() {
        return actualEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = actualEntry;
        saveActualEntryAndMoveNext();
        return result;
    }

    // Находим и сохраняем самую новую и не удаленную запись, чтобы отдать на следующей итерации
    private void saveActualEntryAndMoveNext() {
        Entry<MemorySegment> localActualEntry = null;

        while (!iterators.isEmpty() && localActualEntry == null) {
            PeekingPriorityIterator iterator = iterators.poll();
            localActualEntry = iterator.getCurrent();
            refreshIfHasNext(iterator);

            // Двигаем все итераторы с указанным значением, т.к. в них хранятся более старые записи
            while (!iterators.isEmpty()
                    && msComparator.compare(iterators.peek().getCurrent().key(), localActualEntry.key()) == 0) {
                var iteratorWithSameEntryKey = iterators.remove();
                refreshIfHasNext(iteratorWithSameEntryKey);
            }

            localActualEntry = localActualEntry.value() == null ? null : localActualEntry;
        }

        actualEntry = localActualEntry;
    }

    private void refreshIfHasNext(PeekingPriorityIterator iterator) {
        if (iterator.hasNext()) {
            iterator.next();
            iterators.add(iterator);
        }
    }
}
