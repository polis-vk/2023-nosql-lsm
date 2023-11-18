package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.inmemory.Memtable;
import ru.vk.itmo.smirnovdmitrii.outofmemory.OutMemoryDao;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Flusher implements Closeable {
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    private final ExecutorService service = Executors.newSingleThreadExecutor();

    public Flusher(final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao) {
        this.outMemoryDao = outMemoryDao;
    }

    public void flush(final Memtable memtable, final Runnable callback) {
        service.execute(() -> {
            try {
                outMemoryDao.flush(memtable);
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                callback.run();
            }
        });
    }

    @Override
    public void close() {
        service.close();
    }
}
