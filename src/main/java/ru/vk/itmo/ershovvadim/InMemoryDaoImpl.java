package ru.vk.itmo.ershovvadim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

public class InMemoryDaoImpl extends AbstractMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return db.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return db.values().iterator();
        } else if (to == null) {
            return db.tailMap(from).values().iterator();
        } else if (from == null) {
            return db.headMap(to).values().iterator();
        }
        return db.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        db.put(entry.key(), entry);
    }
}
