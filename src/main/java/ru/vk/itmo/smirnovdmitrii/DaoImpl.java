package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.util.BinaryMinHeap;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;
import ru.vk.itmo.smirnovdmitrii.util.MinHeap;
import ru.vk.itmo.smirnovdmitrii.util.PeekingIterator;
import ru.vk.itmo.smirnovdmitrii.util.WrappedIterator;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    final InMemoryDao<MemorySegment, Entry<MemorySegment>> inMemoryDao;
    final OutMemoryDao<MemorySegment, Entry<MemorySegment>> outMemoryDao;
    final Comparator<MemorySegment> comparator = MemorySegmentComparator.getInstance();

    private class DaoIterator implements Iterator<Entry<MemorySegment>> {

        private final MinHeap<PeekingIterator<Entry<MemorySegment>>> heap
                = new BinaryMinHeap<PeekingIterator<Entry<MemorySegment>>>(this::compare);

        public void add(final PeekingIterator<Entry<MemorySegment>> iterator) {
            heap.add(iterator);
        }

        private int compare(
                final PeekingIterator<Entry<MemorySegment>> o1,
                final PeekingIterator<Entry<MemorySegment>> o2
        ) {
            final int keyCompare = comparator.compare(o1.peek().key(), o2.peek().key());
            if (keyCompare == 0) {
                return Integer.compare(o1.getId(), o2.getId());
            }
            return keyCompare;
        }

        @Override
        public boolean hasNext() {
            return heap.min() != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            while (true) {
                final PeekingIterator<Entry<MemorySegment>> iterator = heap.removeMin();
                final Entry<MemorySegment> result = iterator.next();
                if (iterator.hasNext()) {
                    heap.add(iterator);
                }
                final MemorySegment key = result.key();
                while (true) {
                    final PeekingIterator<Entry<MemorySegment>> currentIterator = heap.removeMin();
                    if (comparator.compare(currentIterator.peek().key(), key) != 0) {
                        heap.add(currentIterator);
                        break;
                    }
                    currentIterator.next();
                    heap.add(currentIterator);
                }
                if (result.value() != null) {
                    return result;
                }
            }
        }
    }

    public DaoImpl() {
        inMemoryDao = new InMemoryDaoImpl();
        outMemoryDao = new FileDao();
    }

    public DaoImpl(final Config config) {
        inMemoryDao = new InMemoryDaoImpl();
        outMemoryDao = new FileDao(config.basePath());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        int id = 0;
        final PeekingIterator<Entry<MemorySegment>> inMemoryIterator = new WrappedIterator<>(id++, inMemoryDao.get(from, to));
        final DaoIterator daoIterator = new DaoIterator();
        daoIterator.add(inMemoryIterator);
        for (final Iterator<Entry<MemorySegment>> outMemoryIterator: outMemoryDao.get(from, to)) {
            daoIterator.add(new WrappedIterator<>(id++, outMemoryIterator));
        }
        return daoIterator;
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final Entry<MemorySegment> result = inMemoryDao.get(key);
        if (result == null) {
            return outMemoryDao.get(key);
        }
        return result;
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        inMemoryDao.upsert(entry);
    }

    @Override
    public void flush() throws IOException {
        outMemoryDao.save(inMemoryDao.getMap());
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
