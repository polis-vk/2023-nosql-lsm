package ru.vk.itmo.belonogovnikolay;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

public final class InMemoryTreeDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> arena;

    private InMemoryTreeDao() {
        this.arena = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    public static Dao<MemorySegment, Entry<MemorySegment>> newInstance() {
        return new InMemoryTreeDao();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return this.arena.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return this.arena.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (Objects.isNull(from) && Objects.isNull(to)) {
            return this.arena.values().iterator();
        } else if (Objects.isNull(from)) {
            return allTo(to);
        } else if (Objects.isNull(to)) {
            return allFrom(from);
        }

        return this.arena.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return this.arena.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (Objects.isNull(entry)) {
            return;
        }

        this.arena.put(entry.key(), entry);
    }
}
