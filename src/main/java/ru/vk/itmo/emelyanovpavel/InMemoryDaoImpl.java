package ru.vk.itmo.emelyanovpavel;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(
            (o1, o2) -> {
                long offset = o1.mismatch(o2);
                if (offset == -1) {
                    return 0;
                }
                if (offset == o1.byteSize()) {
                    return -1;
                }
                if (offset == o2.byteSize()) {
                    return 1;
                }
                return Byte.compare(o1.get(JAVA_BYTE, offset), o2.get(JAVA_BYTE, offset));
            });

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return all();
        }
        if (from == null) {
            return allTo(to);
        }
        if (to == null) {
            return allFrom(from);
        }
        return storage.subMap(from, to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return storage.tailMap(from)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return storage.headMap(to)
                .values()
                .iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return storage.values()
                .iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

}
