package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.table.KeyComparator;
import ru.vk.itmo.novichkovandrew.table.MemTable;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final MemTable memTable;

    public InMemoryDao() {
        this.memTable = new MemTable(new KeyComparator());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return memTable.tableIterator(from, true, to, false);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memTable.tableIterator(key, true, key, true).next();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.upsert(entry);
    }

    @Override
    public void close() throws IOException {
        memTable.close();
    }
}
