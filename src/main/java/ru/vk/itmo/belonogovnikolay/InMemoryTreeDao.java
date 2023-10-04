package ru.vk.itmo.belonogovnikolay;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The class is an implementation of in memory persistent DAO.
 *
 * @author Belonogov Nikolay
 */
public final class InMemoryTreeDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> arena;
    private PersistenceHelper persistenceHelper;

    private InMemoryTreeDao() {
        this.arena = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    private InMemoryTreeDao(Config config) {
        this(); // commented in PR.
        this.persistenceHelper = PersistenceHelper.newInstance(config.basePath());
        //this.arena = persistenceHelper.readEntries(); commented in PR too.
        // will be using with logging.
    }

    public static Dao<MemorySegment, Entry<MemorySegment>> newInstance() {
        return new InMemoryTreeDao();
    }

    public static Dao<MemorySegment, Entry<MemorySegment>> newInstance(Config config) {
        return new InMemoryTreeDao(config);
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
        Entry<MemorySegment> entry = this.arena.get(key);
        if (Objects.isNull(entry)) {
            try {
                entry = persistenceHelper.readEntry(key);
            } catch (IOException e) { // will be using with logging.
                return null;
            }
        }
        return entry;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (Objects.isNull(entry)) {
            return;
        }
        this.arena.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        persistenceHelper.writeEntries(this.arena);// will be using with logging.
    }
}
