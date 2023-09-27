package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(
            (o1, o2) -> Arrays.compare(o1.toArray(ValueLayout.JAVA_BYTE), o2.toArray(ValueLayout.JAVA_BYTE))
    );

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (to == null && from == null) {
            return this.storage.values().iterator();
        } else if (to == null) {
            return this.storage.tailMap(from).sequencedValues().iterator();
        } else if (from == null) {
            return this.storage.headMap(to).sequencedValues().iterator();
        } else {
            return this.storage.subMap(from, to).sequencedValues().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return this.storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        this.storage.put(entry.key(), entry);
    }
}

