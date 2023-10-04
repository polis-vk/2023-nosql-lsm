package ru.vk.itmo.savkovskiyegor;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistantDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable;
    private final SSTable ssTable;

    public PersistantDao(Config config) throws IOException {
        this.memoryTable = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        this.ssTable = new SSTable(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memoryTable.values().iterator();
        }

        if (to == null) {
            return memoryTable.tailMap(from).values().iterator();
        }

        if (from == null) {
            return memoryTable.headMap(to).values().iterator();
        }

        return memoryTable.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var res = memoryTable.get(key);
        if (res == null) {
            return ssTable.get(key);
        }

        return res;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memoryTable.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        if (!memoryTable.isEmpty()) {
            ssTable.save(memoryTable);
        }
    }
}
