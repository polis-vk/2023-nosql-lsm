package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {

    PriorityQueue<PeekIterator> iterators;

    Entry<MemorySegment> currentEntry;

    MergeIterator(
            List<PeekIterator> fileIterators
    ) {
        iterators = new PriorityQueue<>();
        iterators.addAll(fileIterators);
        currentEntry = getCurrentEntry();
        updateIterators();
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> returnEntry = currentEntry;
        updateCurrentEntry();
        return returnEntry;
    }

    private Entry<MemorySegment> getCurrentEntry() {
        return iterators.poll().getCurrentEntry();
    }

    private void updateIterators() {
        while (!iterators.isEmpty()) {
            PeekIterator iterator = iterators.poll();
            if (iterator.hasNext()) {
                iterator.next();
                iterators.add(iterator);
            }
        }
    }

    private void updateCurrentEntry() {
        updateIterators();
        currentEntry = getCurrentEntry();
    }

}
