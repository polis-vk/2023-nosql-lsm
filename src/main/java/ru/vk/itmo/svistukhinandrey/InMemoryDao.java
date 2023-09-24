package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.svistukhinandrey.Utils;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    TreeMap<MemorySegment, Entry<MemorySegment>> memorySegmentTreeMap;

    public InMemoryDao() {
        memorySegmentTreeMap = new TreeMap<>(Comparator.comparing(x -> {
            if (x == null) return null;
            return Utils.transform(x);
        }));
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
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Entry<MemorySegment> next() {
                    return null;
                }
            };
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
