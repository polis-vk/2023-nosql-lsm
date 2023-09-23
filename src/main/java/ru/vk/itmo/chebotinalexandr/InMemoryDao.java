package ru.vk.itmo.chebotinalexandr;


import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SortedMap<MemorySegment, Entry<MemorySegment>> entries = new ConcurrentSkipListMap<>(
            (o1, o2) -> {
                long i = o1.mismatch(o2);

                if (i >= 0)
                    return Byte.compare(
                            o1.get(ValueLayout.JAVA_BYTE, i),
                            o2.get(ValueLayout.JAVA_BYTE, i)
                    );

                return 0;
            }
    );

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null)
            return all();
        else if (from == null)
            return allTo(to);
        else if (to == null)
            return allFrom(from);
        else
            return entries.subMap(from, to).values().iterator();
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
