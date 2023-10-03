package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class AbstractDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected final NavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    public AbstractDao() {
        storage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        SortedMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null && to == null) {
            subMap = storage;
        } else if (from == null) {
            subMap = storage.headMap(to);
        } else if (to == null) {
            subMap = storage.tailMap(from);
        } else {
            subMap = storage.subMap(from, to);
        }

        return subMap.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

}
