package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> memorySegmentComparatorImpl = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp =
            new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);

    private final SSTablesController controller;

    public InMemoryDao() {
        this.controller = new SSTablesController(memorySegmentComparatorImpl);
    }
    public InMemoryDao(Config conf) {
        this.controller = new SSTablesController(conf.basePath(), memorySegmentComparatorImpl);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        SortedMap<MemorySegment, Entry<MemorySegment>> dataSlice;

        if (from == null && to == null) {
            dataSlice = mp;
        } else if (from == null) {
            dataSlice = mp.headMap(to);
        } else if (to == null) {
            dataSlice = mp.tailMap(from);
        } else {
            dataSlice = mp.subMap(from, to);
        }

        return new SSTableIterator(dataSlice.values().iterator(), controller, from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (mp.containsKey(key)) {
            return mp.get(key);
        }

        return controller.getRow(controller.searchInSStables(key));
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mp.put(entry.key(), entry);
    }


    @Override
    public void close() throws IOException {
        controller.dumpMemTableToSStable(mp);
        mp.clear();
    }
}
