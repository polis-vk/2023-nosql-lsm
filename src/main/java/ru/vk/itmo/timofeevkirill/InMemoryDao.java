package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    MemorySegmentComparator msComparator = new MemorySegmentComparator();
    ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries =
            new ConcurrentSkipListMap<>(msComparator);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegmentEntries.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentEntries.values().iterator();
        } else if (from == null) {
            return memorySegmentEntries.headMap(to).values().iterator();
        } else if (to == null) {
            return memorySegmentEntries.tailMap(from).values().iterator();
        } else {
            return memorySegmentEntries.tailMap(from).headMap(to).values().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntries.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegmentEntries.values().iterator();
    }
}
