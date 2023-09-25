package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    MemorySegmentComparator msComparator = new MemorySegmentComparator();
    ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntries = new ConcurrentSkipListMap<>(msComparator);

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegmentEntries.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<MemorySegment> i = memorySegmentEntries.keySet().iterator();
        return new Iterator<>() {
            MemorySegment entry;

            @Override
            public boolean hasNext() {
                if (!i.hasNext()) return false;
                entry = i.next();
                while (from != null && msComparator.compare(entry, from) < 0 ||
                        to != null && msComparator.compare(entry, to) >= 0) {
                    if (!i.hasNext()) return false;
                    entry = i.next();
                }
                return true;
            }

            @Override
            public Entry<MemorySegment> next() {
                return memorySegmentEntries.get(entry);
            }
        };
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntries.put(entry.key(), entry);
    }
}
