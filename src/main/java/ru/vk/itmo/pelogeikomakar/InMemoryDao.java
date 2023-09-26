package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>>(new MemorySegmentComparator());

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> entryIterator;
        if (from == null && to == null) {
            entryIterator = this.map.values().iterator();
        } else if (from == null) {
            entryIterator = this.map.headMap(to).values().iterator();
        } else if (to == null) {
            entryIterator = this.map.tailMap(from).values().iterator();
        } else {
            entryIterator = this.map.subMap(from, to).values().iterator();
        }
        return entryIterator;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (key == null) {
            return null;
        }
        return this.map.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }
        if (entry.key() == null) {
            return;
        }
        this.map.put(entry.key(), entry);
    }
}
