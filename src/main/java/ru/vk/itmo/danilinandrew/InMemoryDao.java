package ru.vk.itmo.danilinandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return data.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        MemorySegment updatedFrom = (from == null) ? MemorySegment.NULL : from;

        if (to == null) {
            return data.tailMap(updatedFrom).values().iterator();
        }

        return data.subMap(updatedFrom, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }

        data.put(entry.key(), entry);
    }
}
