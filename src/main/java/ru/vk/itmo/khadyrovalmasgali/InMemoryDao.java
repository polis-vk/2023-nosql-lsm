package ru.vk.itmo.khadyrovalmasgali;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment,
            Entry<MemorySegment>> data = new ConcurrentSkipListMap<>((o1, o2) -> {
        long mismatch = o1.mismatch(o2);
        if (mismatch == o2.byteSize()) {
            return 1;
        } else if (mismatch == o1.byteSize()) {
            return -1;
        } else if (mismatch == -1) {
            return 0;
        }
        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, mismatch),
                o2.get(ValueLayout.JAVA_BYTE, mismatch));
    });

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return data.values().iterator();
        } else if (from == null) {
            return data.headMap(to).values().iterator();
        } else if (to == null) {
            return data.tailMap(from).values().iterator();
        } else {
            return data.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return data.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry != null) {
            data.put(entry.key(), entry);
        }
    }
}
