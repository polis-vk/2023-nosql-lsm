package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class BaseDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable;

    protected BaseDao() {
        this.memoryTable = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public abstract Entry<MemorySegment> get(MemorySegment key);

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memoryTable.values().iterator();
        }

        if (from == null) {
            return memoryTable.headMap(to).values().iterator();
        }
        if (to == null) {
            return memoryTable.tailMap(from).values().iterator();
        }

        return memoryTable.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memoryTable.put(entry.key(), entry);
    }

    public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> getMemoryTable() {
        return memoryTable;
    }
}
