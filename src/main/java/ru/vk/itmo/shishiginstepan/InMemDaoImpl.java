package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InMemDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ExecutorService executor;
    private final Lock flushLock = new ReentrantLock();
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

    private final AtomicLong memStorageSize = new AtomicLong(0);

    private final long memStorageLimit;
    /**
     * TODO не забыть читать эту таблицу !!!!!! СДЕЛАТЬ НА АТОМИК РЕФЕРЕНСАХ????
     */

    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> tempStorage =
            new AtomicReference<>(
                    new ConcurrentSkipListMap<>(
                            keyComparator
                    )
            );
    private final AtomicReference<ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>>> memStorage =
            new AtomicReference<>(
                    new ConcurrentSkipListMap<>(
                            keyComparator
                    )
            );

    private final PersistentStorage persistentStorage;
    private final Path basePath;

    public InMemDaoImpl(Path basePath, long memStorageLimit) {
        this.basePath = basePath;
        this.persistentStorage = new PersistentStorage(this.basePath);
        this.memStorageLimit = memStorageLimit;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public InMemDaoImpl() {
        this.basePath = Paths.get("./");
        this.persistentStorage = new PersistentStorage(this.basePath);
        this.memStorageLimit = Runtime.getRuntime().freeMemory();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memIterator;
        if (to == null && from == null) {
            memIterator = this.memStorage.get().values().iterator();
        } else if (to == null) {
            memIterator = this.memStorage.get().tailMap(from).sequencedValues().iterator();
        } else if (from == null) {
            memIterator = this.memStorage.get().headMap(to).sequencedValues().iterator();
        } else {
            memIterator = this.memStorage.get().subMap(from, to).sequencedValues().iterator();
        }
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        iterators.add(memIterator);
        persistentStorage.enrichWithPersistentIterators(from, to, iterators);
        return new SkipDeletedIterator(
                new MergeIterator(iterators)
        );
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = this.memStorage.get().get(key);
        if (entry == null) {
            entry = this.tempStorage.get().get(key);
        }
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
        this.memStorageSize.updateAndGet((size) -> size + (entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize())));
        this.memStorage.get().put(entry.key(), entry);
        if (this.memStorageSize.get() > this.memStorageLimit) {
            flush();
        }
    }

    @Override
    public void close() {
        this.flush();
        this.persistentStorage.close();
        executor.close();
        // дать закончить все операции
    }

    @Override
    public void flush() {
        executor.execute(() -> {
            if (flushLock.tryLock()) {
                try {
                    if (!this.memStorage.get().isEmpty()) {
                        this.tempStorage.set(this.memStorage.get());
                        this.persistentStorage.store(this.tempStorage.get().values());
                        this.memStorage.set(new ConcurrentSkipListMap<>(keyComparator));
                        this.tempStorage.get().clear();
                        this.memStorageSize.set(0);
                    }
                } finally {
                    flushLock.unlock();
                }
            } else {
                throw new RuntimeException("flush already in process");
            }
        });
    }

    @Override
    public void compact() {
        persistentStorage.compact();
    }
}
