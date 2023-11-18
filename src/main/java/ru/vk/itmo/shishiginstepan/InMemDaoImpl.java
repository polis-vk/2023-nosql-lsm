package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final Comparator<MemorySegment> keyComparator = (o1, o2) -> {
        long mismatch = o1.mismatch(o2);
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
    };
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memStorage = new ConcurrentSkipListMap<>(
            keyComparator
    );

    private final PersistentStorage persistentStorage;
    private final Path basePath;

    public InMemDaoImpl(Path basePath) {
        this.basePath = basePath;
        this.persistentStorage = new PersistentStorage(this.basePath);
    }

    public InMemDaoImpl() {
        this.basePath = Paths.get("./");
        this.persistentStorage = new PersistentStorage(this.basePath);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memIterator;
        if (to == null && from == null) {
            memIterator = this.memStorage.values().iterator();
        } else if (to == null) {
            memIterator = this.memStorage.tailMap(from).sequencedValues().iterator();
        } else if (from == null) {
            memIterator = this.memStorage.headMap(to).sequencedValues().iterator();
        } else {
            memIterator = this.memStorage.subMap(from, to).sequencedValues().iterator();
        }
        List<Iterator<Entry<MemorySegment>>> persistentIterators = this.persistentStorage.get(from, to, memIterator);
        return new SkipDeletedIterator(
                new MergeIterator(persistentIterators)
        );
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = this.memStorage.get(key);
        if (entry == null) {
            entry = persistentStorage.get(key);
        }
        if (entry != null && entry.value() == null) {
            return null;
        }
        return entry;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        this.memStorage.put(entry.key(), entry);
    }

    @Override
    public void close() {
        this.flush();
        this.persistentStorage.close();
    }

    @Override
    public void flush() {
        if (!this.memStorage.isEmpty()) {
            this.persistentStorage.store(this.memStorage.values());
        }
        this.memStorage.clear();
    }

    @Override
    public void compact() {
        persistentStorage.compact(this.get(null, null));
    }
}
