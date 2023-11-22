package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class PesistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Config config;

    private Environment environment;
    private final ExecutorService executor;

    private final Lock readLock;
    private final Lock writeLock;

    private final AtomicBoolean isFlushing;
    private final AtomicBoolean isCompacting;

    private static final Logger logger = Logger.getLogger(PesistentDao.class.getName());

    public PesistentDao(Config config) throws IOException {
        this.config = config;

        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();

        this.isFlushing = new AtomicBoolean(false);
        this.isCompacting = new AtomicBoolean(false);

        this.executor = Executors.newCachedThreadPool();
        this.environment = new Environment(
                new ConcurrentSkipListMap<>(Tools::compare),
                config.basePath()
        );
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return environment.range(from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        Environment currEnv;
        readLock.lock();
        try {
            currEnv = environment;
        } finally {
            readLock.unlock();
        }

        if (currEnv.getBytes() > config.flushThresholdBytes() && isFlushing.get()) {
            throw new IllegalStateException("Table is full, flushing in process.");
        }

        if (currEnv.put(entry) <= config.flushThresholdBytes()) {
            return;
        }

        try {
            flush();
        } catch (IOException e) {
            logger.severe("Flush error.");
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Environment currEnv;
        readLock.lock();
        try {
            currEnv = environment;
        } finally {
            readLock.unlock();
        }

        Entry<MemorySegment> memTableEntry = currEnv.getMemTableEntry(key);
        Entry<MemorySegment> flushingTableEntry = currEnv.getFlushingTableEntry(key);

        if (memTableEntry != null) {
            return Tools.entryToReturn(memTableEntry);
        }
        if (flushingTableEntry != null) {
            return Tools.entryToReturn(flushingTableEntry);
        }

        Iterator<Entry<MemorySegment>> iterator = currEnv.range(key, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (Tools.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    @Override
    public void compact() throws IOException {
        // Должны гарантировать, что один поток будет выполнять компакт в фоне.
        boolean tryToCompact = isCompacting.compareAndSet(false, true);
        if (!tryToCompact) {
            // Считаем, что уже компактим.
            return;
        }

        executor.execute(() -> {
            try {
                environment.compact();
                isCompacting.set(false);
            } catch (IOException e) {
                logger.severe("Compact error.");
            }
        });
    }

    @Override
    public void flush() throws IOException {
        // Должны гарантировать, что один поток будет выполнять флаш в фоне.
        boolean tryToFlush = isFlushing.compareAndSet(false, true);
        if (!tryToFlush) {
            // Считаем, что уже флашим.
            return;
        }

        writeLock.lock();
        try {
            this.environment = new Environment(environment.getTable(), config.basePath());
        } finally {
            writeLock.unlock();
        }

        executor.execute(() -> {
            try {
                environment.flush();
            } catch (IOException e) {
                logger.severe("Flush error.");
            }
            isFlushing.set(false);
        });
    }

    @Override
    public void close() throws IOException {
        // Ожидаем выполнения фоновых flush и сompact.
        executor.close();

        this.environment = new Environment(environment.getTable(), config.basePath());
        environment.flush();
    }
}
