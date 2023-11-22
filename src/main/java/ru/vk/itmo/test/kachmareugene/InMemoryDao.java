package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected final Comparator<MemorySegment> memorySegmentComparatorImpl = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp =
            new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);
    protected Config conf;
    protected AtomicLong byteSize = new AtomicLong(0L);
    protected final SSTablesController controller;

    public InMemoryDao() {
        this.controller = new SSTablesController(new MemSegComparatorNull());
    }

    public InMemoryDao(Config conf) {
        this.conf = conf;
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

        return dataSlice.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (mp.containsKey(key)) {
            if (mp.get(key).value() == null) {
                return null;
            }
            return mp.get(key);
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
        if (entry.value() == null) {
            byteSize.addAndGet(entry.key().byteSize());
            return;
        }
        byteSize.addAndGet(entry.key().byteSize() + entry.value().byteSize());
    }

    @Override
    public void close() throws IOException {
        close(mp);
        controller.closeArena();
    }

    @Override
    public void flush() throws IOException {
        controller.addFileToLists(close(mp));
    }

    protected Path close(Map<MemorySegment, Entry<MemorySegment>> table) throws IOException {
        try {
            return controller.dumpIterator(table.values()).first;
        } finally {
            mp.clear();
        }
    }

    protected void closeMemTable() {
        mp.clear();
    }
    protected SortedMap<MemorySegment, Entry<MemorySegment>> getMemTable() {
        return mp;
    }

}
