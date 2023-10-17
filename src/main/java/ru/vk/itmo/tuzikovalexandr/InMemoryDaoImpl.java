package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memory =
            new ConcurrentSkipListMap<>(MemorySegmentComparator::compare);
    private final SSTable ssTable;

    public InMemoryDaoImpl(Config config) throws IOException {
        this.ssTable = new SSTable(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memoryIterator;

        if (from == null && to == null) {
            memoryIterator = memory.values().iterator();
        } else if (from == null) {
            memoryIterator = memory.headMap(to, false).values().iterator();
        } else if (to == null) {
            memoryIterator = memory.tailMap(from, true).values().iterator();
        } else {
            memoryIterator = memory.subMap(from, true, to, false).values().iterator();
        }

        if (ssTable.isNullIndexList()) {
            return memoryIterator;
        }

        List<PeekIterator> iterators = ssTable.readDataFromTo(from, to);
        iterators.add(new PeekIterator(memoryIterator, ssTable.getIndexListSize() + 1));
        return new RangeIterator(iterators);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entry = memory.get(key);

        if (entry == null) {
            entry = ssTable.readData(key);
        }

        if (entry != null && entry.value() == null) {
            return null;
        }

        return entry;
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
