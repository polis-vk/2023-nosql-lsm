package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage;

    public InMemoryDao() {
        inMemoryStorage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return inMemoryStorage.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return inMemoryStorage.sequencedValues().iterator();
        }

        boolean isLastInclusive = false;
        MemorySegment localFrom = (from == null) ? inMemoryStorage.firstKey() : from;
        MemorySegment localTo = to;

        if (to == null) {
            localTo = inMemoryStorage.lastKey();
            isLastInclusive = true;
        }

        return inMemoryStorage.subMap(localFrom, true, localTo, isLastInclusive).sequencedValues().iterator();
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        inMemoryStorage.put(entry.key(), entry);
    }
}
