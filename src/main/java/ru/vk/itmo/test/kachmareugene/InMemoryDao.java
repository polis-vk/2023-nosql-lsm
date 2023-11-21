package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> memorySegmentComparatorImpl = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp =
            new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);

    private final SSTablesController controller;

    public InMemoryDao() {
        this.controller = new SSTablesController(new MemSegComparatorNull());
    }

    public InMemoryDao(Config conf) {
        this.controller = new SSTablesController(conf.basePath(), new MemSegComparatorNull());
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
        if (key == null) {
            return null;
        }
        Entry<MemorySegment> value = mp.get(key);
        if (value != null) {
            return value.value() == null ? null : value;
        }
        var res = controller.getRow(controller.searchInSStables(key));
        if (res == null) {
            return null;
        }
        return res.value() == null ? null : res;
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
