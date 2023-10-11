package ru.vk.itmo.belonogovnikolay;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The class is an implementation of in memory persistent DAO.
 *
 * @author Belonogov Nikolay
 */
public final class InMemoryTreeDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTable;
    private PersistenceHelper persistenceHelper;

    private InMemoryTreeDao() {
        this.memTable = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
    }

    private InMemoryTreeDao(Config config) {
        this();
        this.persistenceHelper = PersistenceHelper.newInstance(config.basePath());
    }

    public static Dao<MemorySegment, Entry<MemorySegment>> newInstance() {
        return new InMemoryTreeDao();
    }

    public static Dao<MemorySegment, Entry<MemorySegment>> newInstance(Config config) {
        return new InMemoryTreeDao(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return this.memTable.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return this.memTable.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return this.memTable.values().iterator();
        } else if (from == null) {
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        }

        return this.memTable.subMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = this.memTable.get(key);
        if (entry == null) {
            try {
                entry = persistenceHelper.readEntry(key);
            } catch (IOException e) {
                return null;
            }
        }
        return entry;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }
        this.memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        persistenceHelper.writeEntries(this.memTable);
    }
}
