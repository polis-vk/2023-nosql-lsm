package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.grunskiialexey.MemorySegmentDaoFactory;

import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final MemorySegmentDaoFactory factory = new MemorySegmentDaoFactory();
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> data =
            new ConcurrentSkipListMap<>((o1, o2) -> factory.toString(o1).compareTo(factory.toString(o2)));

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return data.values().iterator();
        } else if (from == null) {
            return data.headMap(to).values().iterator();
        } else if (to == null) {
            return data.tailMap(from).values().iterator();
        } else {
            return data.subMap(from, to).values().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }
}
