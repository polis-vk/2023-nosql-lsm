package ru.vk.itmo.volkovnikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries;
    private final SSTable ssTable;

    public DaoImpl(Config config) throws IOException {
        this.memorySegmentEntries = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        this.ssTable = new SSTable(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entry = memorySegmentEntries.get(key);
        return entry == null ? ssTable.readMemoryData(key) : entry;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentEntries.values().iterator();
        }
        if (from == null) {
            return memorySegmentEntries.headMap(to).values().iterator();
        }
        if (to == null) {
            return memorySegmentEntries.tailMap(from).values().iterator();
        }

        return memorySegmentEntries.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntries.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegmentEntries.values().iterator();
    }

    @Override
    public void close() throws IOException {
        if (memorySegmentEntries.isEmpty()) {
            return;
        }

        ssTable.saveMemoryData(memorySegmentEntries);
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("");
    }
}
