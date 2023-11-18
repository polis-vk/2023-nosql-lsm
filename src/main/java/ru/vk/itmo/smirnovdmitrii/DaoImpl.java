package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.inmemory.InMemoryDao;
import ru.vk.itmo.smirnovdmitrii.inmemory.InMemoryDaoImpl;
import ru.vk.itmo.smirnovdmitrii.outofmemory.FileDao;
import ru.vk.itmo.smirnovdmitrii.outofmemory.OutMemoryDao;
import ru.vk.itmo.smirnovdmitrii.util.EqualsComparator;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;
import ru.vk.itmo.smirnovdmitrii.util.iterators.MergeIterator;
import ru.vk.itmo.smirnovdmitrii.util.iterators.WrappedIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final InMemoryDao<MemorySegment, Entry<MemorySegment>> inMemoryDao;
    private final ExecutorService compactionService = Executors.newSingleThreadExecutor();
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    private final EqualsComparator<MemorySegment> comparator = new MemorySegmentComparator();

    public DaoImpl(final Config config) {
        outMemoryDao = new FileDao(config.basePath());
        inMemoryDao = new InMemoryDaoImpl(config.flushThresholdBytes(), outMemoryDao);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        int id = 0;
        final MergeIterator.Builder<MemorySegment, Entry<MemorySegment>> builder
                = new MergeIterator.Builder<>(comparator);
        for (final Iterator<Entry<MemorySegment>> inMemoryIterator : inMemoryDao.get(from, to)) {
            builder.addIterator(new WrappedIterator<>(id++, inMemoryIterator));
        }
        for (final Iterator<Entry<MemorySegment>> outMemoryIterator : outMemoryDao.get(from, to)) {
            builder.addIterator(new WrappedIterator<>(id++, outMemoryIterator));
        }
        return builder.build();
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        Entry<MemorySegment> result = inMemoryDao.get(key);
        if (result == null) {
            result = outMemoryDao.get(key);
        }
        if (result == null || result.value() == null) {
            return null;
        }
        return result;
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        try {
            inMemoryDao.upsert(entry);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void flush() throws IOException {
        inMemoryDao.flush();
    }

    @Override
    public void compact() {
        compactionService.execute(() -> {
            try {
                outMemoryDao.compact();
            } catch (final IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public void close() throws IOException {
        inMemoryDao.close();
        compactionService.close();
        outMemoryDao.close();
    }
}
