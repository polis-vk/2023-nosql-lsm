package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.svistukhinandrey.Utils;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentTreeMap;

    public InMemoryDao() {
        memorySegmentTreeMap = new ConcurrentSkipListMap<>(Comparator.comparing(Utils::memorySegmentToString));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegmentTreeMap.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (memorySegmentTreeMap.isEmpty()) {
            return Utils.getEmptyIterator();
        }

        if (from == null) {
            from = memorySegmentTreeMap.firstKey();
        }

        boolean last = false;
        if (to == null) {
            to = memorySegmentTreeMap.lastKey();
            last = true;
        }

        return memorySegmentTreeMap.subMap(from, true, to, last).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentTreeMap.put(entry.key(), entry);
    }
}
