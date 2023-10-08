package ru.vk.itmo.kislovdanil.iterators;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, IteratorAndValue> itemsPool;
    private Entry<MemorySegment> currentEntry;

    public MergeIterator(List<DatabaseIterator> iterators, Comparator<MemorySegment> comp) {
        this.itemsPool = new TreeMap<>(comp);
        for (DatabaseIterator iter : iterators) {
            moveIterator(iter);
        }
        updateCurrentEntry();
    }

    private void updateCurrentEntry() {
        MemorySegment value = null;
        MemorySegment key = null;
        while (value == null && !itemsPool.isEmpty()) {
            key = itemsPool.firstKey();
            IteratorAndValue iteratorAndValue = itemsPool.get(key);
            itemsPool.remove(key);
            moveIterator(iteratorAndValue.iterator);
            value = iteratorAndValue.value;
        }
        currentEntry = (value == null) ? null : new BaseEntry<>(key, value);
    }

    private void moveIterator(DatabaseIterator iter) {
        while (iter.hasNext()) {
            Entry<MemorySegment> entry = iter.next();
            if (itemsPool.containsKey(entry.key())) {
                DatabaseIterator concurrentIterator = itemsPool.get(entry.key()).iterator;
                if (iter.getPriority() < concurrentIterator.getPriority()) {
                    continue;
                }
                else {
                    moveIterator(concurrentIterator);
                }
            }
            itemsPool.put(entry.key(), new IteratorAndValue(iter, entry.value()));
            return;
        }
    }

    @Override
    public boolean hasNext() {
        return currentEntry != null;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> result = currentEntry;
        updateCurrentEntry();
        return result;
    }

    private static final class IteratorAndValue {
        private final DatabaseIterator iterator;
        private final MemorySegment value;

        public IteratorAndValue(DatabaseIterator iterator, MemorySegment value) {
            this.iterator = iterator;
            this.value = value;
        }
    }
}
