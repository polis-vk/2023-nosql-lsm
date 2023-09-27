package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SortedMap<MemorySegment, Entry<MemorySegment>> dataBase = new ConcurrentSkipListMap<>((o1, o2) -> {
        if (o1.byteSize() != o2.byteSize()) return Long.compare(o1.byteSize(), o2.byteSize());

        var offset = o1.mismatch(o2);
        if (offset == -1) return 0;
        if (o1.get(JAVA_BYTE, offset) > o2.get(JAVA_BYTE, offset)) return 1;
        else return -1;
    });

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return dataBase.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return dataBase.values().iterator();
        }
        if (from == null) {
            return dataBase.headMap(to).values().iterator();
        }
        if (to == null) {
            return dataBase.tailMap(from).values().iterator();
        }
        return dataBase.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) return;

        dataBase.put(entry.key(), entry);
    }
}
