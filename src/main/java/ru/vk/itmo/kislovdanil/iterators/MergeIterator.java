package ru.vk.itmo.kislovdanil.iterators;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

// Iterates through SSTables and MemTable using N pointers algorithm. Conflicts being solved by iterator priority.
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

    // Get next entry (skip all entries with null value)
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

    // Move iterator to next value keeping invariant (several iterators mustn't point to equal keys at the same time)
    private void moveIterator(DatabaseIterator iter) {
        while (iter.hasNext()) {
            Entry<MemorySegment> entry = iter.next();
            boolean hasConcurrentKey = itemsPool.containsKey(entry.key());
            boolean winPriorityConflict = false;
            DatabaseIterator concurrentIterator = null;
            if (itemsPool.containsKey(entry.key())) {
                concurrentIterator = itemsPool.get(entry.key()).iterator;
                winPriorityConflict = iter.getPriority() > concurrentIterator.getPriority();
            }
            if (winPriorityConflict) {
                moveIterator(concurrentIterator);
            }
            if (!hasConcurrentKey || winPriorityConflict) {
                itemsPool.put(entry.key(), new IteratorAndValue(iter, entry.value()));
                break;
            }
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
