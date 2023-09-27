package ru.vk.itmo.test.kononovvladimir;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.Utils;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> concurrentSkipListMap = new ConcurrentSkipListMap<>(Utils.memorySegmentComparator);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        from = (from == null) ? MemorySegment.NULL : from;

        if (to == null) {
            return concurrentSkipListMap.tailMap(from).values().iterator();
        }

        return concurrentSkipListMap.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) return;

        concurrentSkipListMap.put(entry.key(), entry);
    }
}