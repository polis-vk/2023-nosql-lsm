package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    private final SSTable ssTable;

    private final Path storagePath;

    public InMemoryDaoImpl(Path storagePath) {
        this.storage = new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
        try {
            this.ssTable = SSTable.load(storagePath);
        } catch (IOException e) {
            throw new InMemoryDaoCreationException(e);
        }
        this.storagePath = storagePath;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.sequencedValues().iterator();
        }

        if (from == null) {
            return storage.headMap(to).sequencedValues().iterator();
        }

        if (to == null) {
            return storage.tailMap(from).sequencedValues().iterator();
        }

        return storage.subMap(from, to).sequencedValues().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> fromMemTable = storage.get(key);
        if (fromMemTable != null) {
            return fromMemTable;
        }
        if (ssTable != null) {
            return ssTable.get(key);
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (ssTable != null) {
            ssTable.close();
        }
        SSTable.save(storage.values(), storagePath);
    }
}
