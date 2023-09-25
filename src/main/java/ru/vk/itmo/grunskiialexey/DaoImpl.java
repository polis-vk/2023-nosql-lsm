package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.test.grunskiialexey.MemorySegmentFactory;

import java.lang.foreign.MemorySegment;
import java.util.*;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final TreeMap<MemorySegment, Entry<MemorySegment>> data = new TreeMap<>(new Comparator<>() {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            return factory.toString(o1).compareTo(factory.toString(o2));
        }
    });
    private final MemorySegmentFactory factory = new MemorySegmentFactory();

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return data.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }
}
