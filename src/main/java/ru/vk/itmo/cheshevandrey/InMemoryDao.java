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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Принцип работы в многопоточном режиме:
 * <p>
 * После очередного флаш происходит создание нового состояния (происходит компакт, ждем его выполнения).
 * Ниже описано поведение для текущего состояния.
 * <p>
 * Запрос на компакт:
 * <ol>
 *     <li> Компакт может произойти в любой момент. </li>
 *     <li> Компакт выполняется, была добавлена новая SSTable -> return (должны выполнить сразу после инициализации следующего состояния).</li>
 * </ol>
 * <p>
 * Запрос на флаш:
 * <ol>
 *     <li>Происходит флаш -> return (должны выполнить сразу после инициализации следующего состояния).</li>
 * </ol>
 * <ol>
 *     Проверяем до выполнения:
 *     <li>Был выполнен флаш, компакт происходит -> return (не выполняем флаш, ждем выполнения компакта).</li>
 *     <li>Был выполнен флаш, компакт не происходит -> создаем новое состояние (считаем, что дождались выполнения компакта).</li>
 * </ol>
 * <ol>
 *     Проверяем после выполнения:
 *     <li>Компакт не происходит -> создаем новое состояние.</li>
 *     <li>Компакт происходит -> помечаем, что выполнили флаш, не создаем новое состояние, ждем выполнения компакта.</li>
 * </ol>
 */
public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Config config;
    private final Path path;

    private final Arena arena;
    private Environment environment;
    private final ExecutorService executor;

    private final Lock readLock;
    private final Lock writeLock;

    private AtomicBoolean shouldFlushAfterReloadEnv;
    private AtomicBoolean shouldCompactAfterReloadEnv;
    private static final Logger logger = Logger.getLogger(InMemoryDao.class.getName());

    public InMemoryDao(Config config) throws IOException {
        this.config = config;
        this.path = config.basePath().resolve("data");
        Files.createDirectories(path);

        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.readLock = lock.readLock();
        this.writeLock = lock.writeLock();

        this.shouldFlushAfterReloadEnv = new AtomicBoolean(false);
        this.shouldCompactAfterReloadEnv = new AtomicBoolean(false);

        this.executor = Executors.newCachedThreadPool();

        arena = Arena.ofShared();
        this.environment = new Environment(new ConcurrentSkipListMap<>(Tools::compare), config, arena, readLock, writeLock);
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
            if (currEnv.getMemTableBytes().get() > config.flushThresholdBytes() && currEnv.isFlushing.get()) {
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
            logger.severe("fail to flush");
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

    @Override
    public void compact() throws IOException {
        readLock.lock();
        try {
            if (environment.isCompactingCompleted.get() || environment.isCompacting.get()) {
                if (!environment.getTable().isEmpty()) {
                    shouldCompactAfterReloadEnv.set(true);
                }
                return;
            }
        } finally {
            readLock.unlock();
        }

        backgroundCompact();

        writeLock.lock();
        try {
            if (environment.isFlushingCompleted.get()) {
                this.environment = new Environment(this.environment.getTable(), config, arena, readLock, writeLock);
            } else {
                // Если был выполнен компакт без flush в текущем состоянии,
                // то считаем, что состояние актуальное.
                environment.isCompactingCompleted.set(false);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void flush() throws IOException {
        readLock.lock();
        try {
            if (environment.isFlushingCompleted.get() || environment.isFlushing.get()) {
                if (!environment.getTable().isEmpty()) {
                    shouldFlushAfterReloadEnv.set(true);
                }
                return;
            }
        } finally {
            readLock.unlock();
        }

        backgroundFlush();

        writeLock.lock();
        try {
            if (!environment.isCompacting.get()) {
                this.environment = new Environment(this.environment.getTable(), config, arena, readLock, writeLock);
            }
        } finally {
            writeLock.unlock();
        }
    }

    private void backgroundCompact() {
        executor.execute(() -> {
            try {
                environment.compact();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void backgroundFlush() {
        executor.execute(() -> {
            try {
                environment.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        // Ожидаем выполнения фоновых flush и сompact.
        executor.shutdown();
        try {
            // Взято из метода ExecutorService.close().
            executor.awaitTermination(1L, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            executor.shutdownNow();
        }

        if (!environment.getTable().isEmpty()) {
            backgroundFlush();
        }
        // Если только что был вызван flush, то ожидаем его выполнения и закрываем ExecutorService.
        executor.close();

        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }
}
