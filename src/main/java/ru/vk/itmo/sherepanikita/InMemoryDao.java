package ru.vk.itmo.sherepanikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments;

    private SSTable ssTable;

    public InMemoryDao() {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    public InMemoryDao(Config config) {
        segments = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        ssTable = new SSTable(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segments.values().iterator();
        } else if (from == null) {
            return segments.headMap(to).values().iterator();
        } else if (to == null) {
            return segments.tailMap(from).values().iterator();
        }
        return segments.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        try {
            Entry<MemorySegment> segment = segments.get(key);
            if (segment == null) {
                return ssTable.readData(key);
            } else return segment;
        } catch (IOException ex) {
            ex.printStackTrace();
            return null;
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            throw new IllegalArgumentException("Incoming entry is NULL");
        }
        segments.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        ssTable.writeGivenInMemoryData(segments);
        Dao.super.close();
    }
}
