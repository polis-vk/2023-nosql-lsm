package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class OutMemoryDaoControllerImpl implements OutMemoryDaoController {

    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    private final ExecutorService compactionService = Executors.newSingleThreadExecutor();
    private final Semaphore compactionSemaphore = new Semaphore(1);
    private final ExecutorService flushService = Executors.newSingleThreadExecutor();
    private final Semaphore flushSemaphore = new Semaphore(1);

    public OutMemoryDaoControllerImpl(final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao) {
        this.outMemoryDao = outMemoryDao;
    }

    @Override
    public void compact() {
        if (!flushSemaphore.tryAcquire()) {
            return;
        }
        compactionService.execute(() -> {
            try {
                outMemoryDao.compact();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                compactionSemaphore.release();
            }
        });
    }

    @Override
    public void flush(final Iterable<Entry<MemorySegment>> iterable) {
        if (!flushSemaphore.tryAcquire()) {
            return;
        }
        flushService.execute(() -> {
            try {
                outMemoryDao.flush(iterable);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                flushSemaphore.release();
            }
        });
    }

    @Override
    public void close() {
        flushService.close();
        compactionService.close();
    }
}
