package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.svistukhinandrey.Utils;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memorySegmentTreeMap;
    private final Iterator<Entry<MemorySegment>> emptyIterator = new Iterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<MemorySegment> next() {
            return null;
        }
    };

    public InMemoryDao() {
        memorySegmentTreeMap = new ConcurrentSkipListMap<>(Comparator.comparing(Utils::transform));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();

        if (Utils.transform(next.key()).equals(Utils.transform(key))) {
            return next;
        }

        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        boolean last = false;
        if (!memorySegmentTreeMap.isEmpty()) {
            if (from == null) {
                from = memorySegmentTreeMap.firstKey();
            }
            if (to == null) {
                to = memorySegmentTreeMap.lastKey();
                last = true;
            }

            return memorySegmentTreeMap.subMap(from, true, to, last).values().iterator();
        } else {
            return emptyIterator;
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }

        memorySegmentTreeMap.put(entry.key(), entry);
    }
}
