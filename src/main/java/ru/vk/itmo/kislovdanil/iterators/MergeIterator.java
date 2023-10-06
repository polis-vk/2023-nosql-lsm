package ru.vk.itmo.kislovdanil.iterators;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kislovdanil.iterators.DatabaseIterator;

import java.lang.foreign.MemorySegment;
import java.util.*;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {
    private static final class IteratorAndValue {
        private final DatabaseIterator iterator;
        private final MemorySegment value;

        public IteratorAndValue(DatabaseIterator iterator, MemorySegment value) {
            this.iterator = iterator;
            this.value = value;
        }
    }

    private final NavigableMap<MemorySegment, IteratorAndValue> itemsPool;

    private void handleEntry(DatabaseIterator iter) {
        while (iter.hasNext()) {
            Entry<MemorySegment> entry = iter.next();
            if (itemsPool.containsKey(entry.key()) &&
                    (iter.getPriority() < itemsPool.get(entry.key()).iterator.getPriority())) {
                continue;
            }
            itemsPool.put(entry.key(), new IteratorAndValue(iter, entry.value()));
            return;
        }
    }

    public MergeIterator(List<DatabaseIterator> iterators, Comparator<MemorySegment> comp) {
        this.itemsPool = new TreeMap<>(comp);
        for (DatabaseIterator iter : iterators) {
            handleEntry(iter);
        }
    }


    @Override
    public boolean hasNext() {
        return !this.itemsPool.isEmpty();
    }

    @Override
    public Entry<MemorySegment> next() {
        MemorySegment key = itemsPool.firstKey();
        IteratorAndValue iteratorAndValue = itemsPool.get(key);
        itemsPool.remove(key);
        handleEntry(iteratorAndValue.iterator);
        return new BaseEntry<>(key, iteratorAndValue.value);
    }
}
