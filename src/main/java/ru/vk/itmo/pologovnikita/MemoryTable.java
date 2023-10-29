package ru.vk.itmo.pologovnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryTable implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> segmentToEntry =
            new ConcurrentSkipListMap<>(comparator);

    private SSTable ssTable;

    public MemoryTable() {
        //Empty constructor for only in memory table.
    }

    public MemoryTable(Config config) {
        ssTable = new SSTable(config.basePath());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return segmentToEntry.values().iterator();
        }
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null) {
            subMap = segmentToEntry.headMap(to);
        } else if (to == null) {
            subMap = segmentToEntry.tailMap(from);
        } else {
            subMap = segmentToEntry.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (segmentToEntry.containsKey(key)) {
            return segmentToEntry.get(key);
        }
        if (ssTable == null) {
            return null;
        }
        var value = ssTable.get(key);
        if (value == null) {
            return null;
        }
        return new BaseEntry<>(key, value);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        segmentToEntry.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (ssTable != null) {
            ssTable.save(segmentToEntry);
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        if (ssTable != null) {
            ssTable.close();
        }
    }
}
