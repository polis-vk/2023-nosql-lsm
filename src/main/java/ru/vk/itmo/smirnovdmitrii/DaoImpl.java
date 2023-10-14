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
            if (iterator.hasNext()) {
                heap.add(iterator);
            }
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
            advance();
            return heap.min() != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            advance();
            final PeekingIterator<Entry<MemorySegment>> iterator = heap.removeMin();
            final Entry<MemorySegment> result = iterator.next();
            add(iterator);
            final MemorySegment key = result.key();
            while (!heap.isEmpty()) {
                final PeekingIterator<Entry<MemorySegment>> currentIterator = heap.min();
                if (!equals(currentIterator.peek().key(), key)) {
                    break;
                }
                skip();
            }
            return result;
        }

        private void advance() {
            if (heap.isEmpty()) {
                return;
            }
            MemorySegment deletedKey = null;
            while (!heap.isEmpty()) {
                final PeekingIterator<Entry<MemorySegment>> iterator = heap.min();
                final Entry<MemorySegment> entry = iterator.peek();
                final MemorySegment currentKey = entry.key();
                if (entry.value() == null || equals(deletedKey, currentKey)) {
                    deletedKey = currentKey;
                    skip();
                    continue;
                }
                break;
            }
        }

        private void skip() {
            final PeekingIterator<Entry<MemorySegment>> iterator = heap.removeMin();
            iterator.next();
            add(iterator);
        }

        private static boolean equals(final MemorySegment m1, final MemorySegment m2) {
            if (m1 == null && m2 == null) {
                return true;
            }

            if (m1 == null || m2 == null) {
                return false;
            }
            return m1.mismatch(m2) == -1;
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
        final PeekingIterator<Entry<MemorySegment>> inMemoryIterator
                = new WrappedIterator<>(id++, inMemoryDao.get(from, to));
        final DaoIterator daoIterator = new DaoIterator();
        daoIterator.add(inMemoryIterator);
        for (final Iterator<Entry<MemorySegment>> outMemoryIterator: outMemoryDao.get(from, to)) {
            daoIterator.add(new WrappedIterator<>(id++, outMemoryIterator));
        }
        return daoIterator;
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
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
        outMemoryDao.save(inMemoryDao.getMap());
    }

    @Override
    public void close() throws IOException {
        flush();
    }
}
