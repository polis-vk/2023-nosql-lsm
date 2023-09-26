package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.cheshevandrey.InMemoryFactory;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    ConcurrentSkipListMap<String, Entry<MemorySegment>> map = new ConcurrentSkipListMap<>();
    InMemoryFactory factory = new InMemoryFactory();

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return map.get(factory.toString(key));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (map.size() == 0) {
            return map.values().iterator();
        }

        String first = (from == MemorySegment.NULL) ? map.firstKey() : map.ceilingKey(factory.toString(from));
        String last = null;

        if (to != MemorySegment.NULL) {
            last = map.ceilingKey(factory.toString(to));
        }

        return (last == null)
                ? map.tailMap(first, true).values().iterator() :
                map.subMap(first, last).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        String key = factory.toString(entry.key());
        map.put(key, entry);
    }
}
