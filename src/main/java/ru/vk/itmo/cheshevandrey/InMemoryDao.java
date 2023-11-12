package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Config config;
    private final Path path;

    private final Arena arena;
    private Environment environment;
    private final ExecutorService executor;

    private final Lock readLock;
    private final Lock writeLock;

    private final AtomicBoolean isFlushing;
    private final AtomicBoolean isCompacting;
    private final AtomicBoolean isFlushCompleted;
    private final AtomicBoolean shouldFlushAfterReloadEnv;
    private final AtomicBoolean shouldCompactAfterReloadEnv;

    private static final Logger logger = Logger.getLogger(InMemoryDao.class.getName());

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();

        this.isFlushing = new AtomicBoolean(false);
        this.isCompacting = new AtomicBoolean(false);
        this.isFlushCompleted = new AtomicBoolean(false);
        this.shouldFlushAfterReloadEnv = new AtomicBoolean(false);
        this.shouldCompactAfterReloadEnv = new AtomicBoolean(false);

        this.executor = Executors.newCachedThreadPool();
        arena = Arena.ofShared();
        this.environment = new Environment(new ConcurrentSkipListMap<>(Tools::compare), config, arena);
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
            if (currEnv.getMemTableBytes().get() > config.flushThresholdBytes() && isFlushing.get()) {
                throw new IllegalStateException("table is full, flushing in process");
            }
        } finally {
            readLock.unlock();
        }

        if (currEnv.put(entry) <= config.flushThresholdBytes()) {
            return;
        }

        try {
            flush();
        } catch (IOException e) {
            logger.severe("flush error");
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
        } else if (flushingTableEntry != null) {
            return Tools.entryToReturn(flushingTableEntry);
        }

        Iterator<Entry<MemorySegment>> iterator = currEnv.range(null, null);

        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();
        if (Tools.compare(next.key(), key) == 0) {
            return next;
        }
        return null;
    }

    /**
     * <pre>
     * Компакт выполняется, была добавлена новая SSTable -> return (должны выполнить сразу после инициализации следующего состояния).</pre>
     */
    @Override
    public void compact() throws IOException {
        Environment currEnv;
        readLock.lock();
        try {
            if (isCompacting.get()) {
                if (isFlushCompleted.get()) {
                    // Запомнили, что должны будем опять запустить compact().
                    shouldCompactAfterReloadEnv.set(true);
                    // Считаем, что уже учли выполненные флаши.
                    isFlushCompleted.set(false);
                }
                return;
            }

            // Могут выполнять несколько потоков.
            // Должны гарантировать, что один поток будет выполнять флаш.
            boolean tryToCompact = isCompacting.compareAndSet(false, true);
            if (!tryToCompact) {
                return;
            }
            currEnv = environment;
        } finally {
            readLock.unlock();
        }

        executor.execute(() -> {
            try {
                currEnv.compact();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        writeLock.lock();
        try {
            currEnv.completeCompact();
            isCompacting.set(false);
            if (shouldCompactAfterReloadEnv.get()) {
                // Вызываем новый compact() из другого потока, чтобы не блокироваться.
                callCompactFromAnotherThread();
                shouldCompactAfterReloadEnv.set(false);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void callCompactFromAnotherThread() {
        executor.execute(() -> {
            try {
                compact();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * <pre>
     * Происходит флаш и на текущий момент таблица непустая ->
     * return (должны выполнить сразу после инициализации следующего состояния).
     *
     * После выполнения создаем новое окружение.</pre>
     */
    @Override
    public void flush() throws IOException {
        readLock.lock();
        try {
            if (isFlushing.get()) {
                if (!environment.getTable().isEmpty()) {
                    shouldFlushAfterReloadEnv.set(true);
                }
                return;
            }

            // Могут выполнять несколько потоков.
            // Должны гарантировать, что один поток будет выполнять флаш.
            boolean tryToFlush = isFlushing.compareAndSet(false, true);
            if (!tryToFlush) {
                return;
            }
        } finally {
            readLock.unlock();
        }


        executor.execute(() -> {
            try {
                environment.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        writeLock.lock();
        try {
            isFlushing.set(false);
            isFlushCompleted.set(true);
            setNewEnvironment();
        } finally {
            writeLock.unlock();
        }
    }

    private void setNewEnvironment() throws IOException {
        this.environment = new Environment(environment.getTable(), config, arena);
        // Вызываем новый flush() из другого потока, чтобы не блокироваться.
        executor.execute(() -> {
            try {
                if (shouldFlushAfterReloadEnv.get()) {
                    shouldFlushAfterReloadEnv.set(false);
                    flush();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        // Ожидаем выполнения фоновых flush и сompact.
        executor.close();

        if (!environment.getTable().isEmpty()) {
            environment.flush();
        }

        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }
}
