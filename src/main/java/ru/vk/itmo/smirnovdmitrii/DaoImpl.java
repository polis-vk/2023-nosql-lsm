package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.EqualsComparator;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;
import ru.vk.itmo.smirnovdmitrii.util.MergeIterator;
import ru.vk.itmo.smirnovdmitrii.util.PeekingIterator;
import ru.vk.itmo.smirnovdmitrii.util.WrappedIterator;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.Objects;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final InMemoryDao<MemorySegment, Entry<MemorySegment>> inMemoryDao;
    private final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    private final EqualsComparator<MemorySegment> comparator = new MemorySegmentComparator();

    public DaoImpl() {
        inMemoryDao = new InMemoryDaoImpl();
        outMemoryDao = new FileDao(this);
    }

    public DaoImpl(final Config config) {
        inMemoryDao = new InMemoryDaoImpl();
        outMemoryDao = new FileDao(this, config.basePath());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        int id = 0;
        final PeekingIterator<Entry<MemorySegment>> inMemoryIterator
                = new WrappedIterator<>(id++, inMemoryDao.get(from, to));
        final MergeIterator.Builder<MemorySegment, Entry<MemorySegment>> builder
                = new MergeIterator.Builder<MemorySegment, Entry<MemorySegment>>()
                .addComparator(comparator)
                .addIterator(inMemoryIterator);
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
        inMemoryDao.upsert(entry);
    }

    @Override
    public void flush() throws IOException {
        outMemoryDao.save(inMemoryDao.commit());
    }

    @Override
    public void compact() throws IOException {
        outMemoryDao.compact();
    }

    @Override
    public void close() throws IOException {
        flush();
        outMemoryDao.close();
        inMemoryDao.close();
    }
}
