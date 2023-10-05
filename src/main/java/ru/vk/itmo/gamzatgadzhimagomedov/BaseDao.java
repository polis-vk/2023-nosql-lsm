package ru.vk.itmo.gamzatgadzhimagomedov;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class BaseDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;

    protected BaseDao() {
        this.memTable = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    private NavigableMap<MemorySegment, Entry<MemorySegment>> subMap(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return memTable.subMap(from, to);
        }
        if (from != null) {
            return memTable.tailMap(from, true);
        }
        if (to != null) {
            return memTable.headMap(to, false);
        }
        return memTable;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return subMap(from, to).values().iterator();
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
