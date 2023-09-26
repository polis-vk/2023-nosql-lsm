package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SortedMap<MemorySegment, Entry<MemorySegment>> dataBase = new ConcurrentSkipListMap<>((o1, o2) -> {
        ByteBuffer o1Buffer = o1.asByteBuffer().asReadOnlyBuffer();
        ByteBuffer o2Buffer = o2.asByteBuffer().asReadOnlyBuffer();

        if (o1.byteSize() != o2.byteSize()) return Long.compare(o1.byteSize(), o2.byteSize());

        while (o1Buffer.remaining() != 0) {
            int cmp = Byte.compare(o1Buffer.get(), o2Buffer.get());
            if (cmp != 0) return cmp;
        }
        return 0;
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
