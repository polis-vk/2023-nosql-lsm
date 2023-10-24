package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    private final SSTable ssTable;

    public InMemoryDaoImpl(Config config) throws IOException {
        this.ssTable = new SSTable(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return memory.values().iterator();
        } else if (from == null) {
            return memory.headMap(to, false).values().iterator();
        } else if (to == null) {
            return memory.subMap(from, true, memory.lastKey(), false).values().iterator();
        } else {
            return memory.subMap(from, true, to, false).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entry = memory.get(key);
        return entry == null ? ssTable.readData(key) : entry;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memory.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memory.values().iterator();
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException("");
    }

    @Override
    public void close() throws IOException {
        if (memory.isEmpty()) {
            return;
        }

        ssTable.saveMemData(memory.values());
    }
}
