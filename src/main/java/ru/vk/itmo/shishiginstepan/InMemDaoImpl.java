package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memStorage = new ConcurrentSkipListMap<>(
            (o1, o2) -> {
                var mismatch = o1.mismatch(o2);
                if (mismatch == -1) {
                    return 0;
                }

                if (mismatch == o1.byteSize()) {
                    return -1;
                }

                if (mismatch == o2.byteSize()) {
                    return 1;
                }
                byte b1 = o1.get(ValueLayout.JAVA_BYTE, mismatch);
                byte b2 = o2.get(ValueLayout.JAVA_BYTE, mismatch);
                return Byte.compare(b1, b2);
            }
    );

    private final PersistentStorage persistentStorage;
    private final Path basePath;

    public InMemDaoImpl(Path basePath) {
        this.basePath = basePath;
        persistentStorage = new PersistentStorage(this.basePath);
    }

    public InMemDaoImpl() {
        this.basePath = Paths.get("./");
        persistentStorage = new PersistentStorage(this.basePath);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (to == null && from == null) {
            return this.memStorage.values().iterator();
        } else if (to == null) {
            return this.memStorage.tailMap(from).sequencedValues().iterator();
        } else if (from == null) {
            return this.memStorage.headMap(to).sequencedValues().iterator();
        } else {
            return this.memStorage.subMap(from, to).sequencedValues().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var val = this.memStorage.get(key);
        if (val == null) {
            return persistentStorage.get(key);
        }
        return val;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        this.memStorage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        persistentStorage.store(memStorage.values());
        memStorage.clear();
    }
}
