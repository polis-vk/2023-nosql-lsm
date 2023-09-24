package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage;
    private static final Iterator<Entry<MemorySegment>> EMPTY_ITERATOR = new Iterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<MemorySegment> next() {
            throw new NoSuchElementException();
        }
    };

    public InMemoryDao() {
        inMemoryStorage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return inMemoryStorage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (inMemoryStorage.isEmpty()) {
            return EMPTY_ITERATOR;
        }

        if (from == null && to == null) {
            return inMemoryStorage.values().iterator();
        }

        MemorySegment localFrom = from;
        MemorySegment localTo = to;
        boolean last = false;

        if (from == null) {
            localFrom = inMemoryStorage.firstKey();
        }
        if (to == null) {
            localTo = inMemoryStorage.lastKey();
            last = true;
        }

        return inMemoryStorage.subMap(localFrom, true, localTo, last).values().iterator();
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        inMemoryStorage.put(entry.key(), entry);
    }
}
