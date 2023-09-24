package ru.vk.itmo.test.proninvalentin;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * In-memory DAO implementations based on the ConcurrentSkipListMap for a thread safety using.
 */
public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memorySegments;

    public InMemoryDao() {
        memorySegments = new ConcurrentSkipListMap<>(new Comparator<MemorySegment>() {
            @Override
            public int compare(MemorySegment o1, MemorySegment o2) {
                if (o1 == null) {
                    return -1;
                } else if (o2 == null) {
                    return 1;
                }

                var mismatchOffset = o1.mismatch(o2);
                if (mismatchOffset == -1) {
                    return 0;
                }

                return o1.get(ValueLayout.JAVA_BYTE, mismatchOffset) > o2.get(ValueLayout.JAVA_BYTE, mismatchOffset)
                        ? 1
                        : -1;
            }
        });
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return memorySegments.headMap(to).sequencedValues().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memorySegments.sequencedValues().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        } else if (from != null && to == null) {
            return allFrom(from);
        } else if (from == null && to != null) {
            return allTo(to);
        } else {
            return memorySegments.tailMap(from, true).headMap(to).sequencedValues().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memorySegments.getOrDefault(key, null);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return memorySegments.tailMap(from, true).sequencedValues().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegments.put(entry.key(), entry);
    }
}
