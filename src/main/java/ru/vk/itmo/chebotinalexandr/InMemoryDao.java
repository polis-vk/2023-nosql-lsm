package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SortedMap<MemorySegment, Entry<MemorySegment>> entries = new ConcurrentSkipListMap<>(
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


                return Byte.compare(
                            o1.get(ValueLayout.JAVA_BYTE, offset),
                            o2.get(ValueLayout.JAVA_BYTE, offset)
                );

            }
    );

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return all();
        } else if (from == null) {
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        } else {
            return entries.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return entries.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return entries.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return entries.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return entries.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        entries.put(entry.key(), entry);
    }
}
