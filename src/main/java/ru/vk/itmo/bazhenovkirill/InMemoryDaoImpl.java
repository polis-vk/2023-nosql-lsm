package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable
            = new ConcurrentSkipListMap<>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to != null) {
                return memTable.headMap(to).values().iterator();
            }
            return memTable.values().iterator();
        } else {
            if (to == null) {
                return memTable.tailMap(from).values().iterator();
            }
            return memTable.subMap(from, true, to, false).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memTable.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

}
