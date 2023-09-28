package ru.vk.itmo.alginavictoria;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> dataMap
            = new ConcurrentSkipListMap<>((s1, s2) -> {
                var mismatch = s1.mismatch(s2);
                if (mismatch == -1) {
                    return 0;
                }
                if (mismatch == s1.byteSize()) {
                    return -1;
                }
                if (mismatch == s2.byteSize()) {
                    return 1;
                }
                return Byte.compare(s1.get(JAVA_BYTE, mismatch), s2.get(JAVA_BYTE, mismatch));
    });

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (to == null && from == null) {
            return dataMap.values().iterator();
        }
        if (from == null) {
            return dataMap.headMap(to).values().iterator();
        }
        if (to == null) {
            return dataMap.tailMap(from).values().iterator();
        }
        return dataMap.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return key == null ? null : dataMap.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null || entry.key() == null) {
            return;
        }
        dataMap.put(entry.key(), entry);
    }
}
