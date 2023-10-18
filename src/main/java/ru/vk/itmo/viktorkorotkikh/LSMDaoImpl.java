package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

public class LSMDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    private final List<SSTable> ssTables;

    private final Path storagePath;

    public LSMDaoImpl(Path storagePath) {
        this.storage = new ConcurrentSkipListMap<>(MemorySegmentComparator.INSTANCE);
        try {
            this.ssTables = SSTable.load(storagePath);
        } catch (IOException e) {
            throw new LSMDaoCreationException(e);
        }
        this.storagePath = storagePath;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<SSTable.SSTableIterator> ssTableIterators =
                ssTables.stream().map(ssTable -> ssTable.iterator(from, to)).toList();
        return MergeIterator.create(lsmPointerIterator(from, to), ssTableIterators);
    }

    private LSMPointerIterator lsmPointerIterator(MemorySegment from, MemorySegment to) {
        return new MemTableIterator(iterator(from, to));
    }

    private Iterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return storage.sequencedValues().iterator();
        }

        if (from == null) {
            return storage.headMap(to).sequencedValues().iterator();
        }

        if (to == null) {
            return storage.tailMap(from).sequencedValues().iterator();
        }

        return storage.subMap(from, to).sequencedValues().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        return iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        ssTables.forEach(SSTable::close);
        SSTable.save(storage.values(), ssTables.size(), storagePath);
    }

    public static final class MemTableIterator extends LSMPointerIterator {
        private final Iterator<Entry<MemorySegment>> iterator;
        private Entry<MemorySegment> current = null;

        private MemTableIterator(Iterator<Entry<MemorySegment>> storageIterator) {
            this.iterator = storageIterator;
            if (iterator.hasNext()) {
                current = iterator.next();
            }
        }

        @Override
        int getPriority() {
            return Integer.MAX_VALUE;
        }

        @Override
        protected MemorySegment getPointerSrc() {
            return current.key();
        }

        @Override
        protected long getPointerSrcOffset() {
            return 0;
        }

        @Override
        boolean isPointerOnTombstone() {
            return current.value() == null;
        }

        @Override
        protected long getPointerSrcSize() {
            return current.key().byteSize();
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<MemorySegment> entry = current;
            current = iterator.hasNext() ? iterator.next() : null;
            return entry;
        }
    }
}
