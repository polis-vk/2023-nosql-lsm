package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final MemorySegment deletionMark = MemorySegment.ofArray("52958832".getBytes(StandardCharsets.UTF_8));
    private static final Comparator<MemorySegment> keyComparator = new Comparator<>() {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
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
    };
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memStorage = new ConcurrentSkipListMap<>(
            keyComparator
    );

    private final PersistentStorage persistentStorage;
    private final Path basePath;

    public InMemDaoImpl(Path basePath) throws IOException {
        this.basePath = basePath;
        this.persistentStorage = new PersistentStorage(this.basePath);
    }

    public InMemDaoImpl() throws IOException {
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
        var persistentIterators = this.persistentStorage.get(from, to);
        persistentIterators.add(0, memIterator);
        return new SkipDeletedIterator(
                new MergeIterator(persistentIterators),
                deletionMark,
                keyComparator
        );
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entry = this.memStorage.get(key);
        if (entry == null) {
            entry = persistentStorage.get(key);
        }
        if (entry == null) {
            return null;
        }
        if (keyComparator.compare(entry.value(), deletionMark) == 0) {
            return null;
        }
        return entry;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            this.memStorage.put(
                    entry.key(),
                    new BaseEntry<>(entry.key(), deletionMark)
            );
        } else {
            this.memStorage.put(entry.key(), entry);
        }
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
}