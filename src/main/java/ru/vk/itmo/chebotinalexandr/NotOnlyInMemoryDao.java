package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class NotOnlyInMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final int SS_TABLE_PRIORITY = 1;
    private final SSTablesStorage ssTablesStorage;
    private volatile SortedMap<MemorySegment, Entry<MemorySegment>> entries =
            new ConcurrentSkipListMap<>(NotOnlyInMemoryDao::comparator);
    private volatile SortedMap<MemorySegment, Entry<MemorySegment>> flushingEntries =
            new ConcurrentSkipListMap<>(NotOnlyInMemoryDao::comparator);
    private AtomicLong remainingMemory;
    private final Config config;
    private volatile boolean isFlushing = false;
    private volatile boolean isClosed = false;

    private final ExecutorService threadExecutor = Executors.newSingleThreadExecutor();


    public static int comparator(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);

        if (offset == -1) {
            return 0;
        }
        if (offset == segment1.byteSize()) {
            return -1;
        }
        if (offset == segment2.byteSize()) {
            return 1;
        }

        return Byte.compare(
                segment1.get(ValueLayout.JAVA_BYTE, offset),
                segment2.get(ValueLayout.JAVA_BYTE, offset)
        );
    }

    public static int entryComparator(Entry<MemorySegment> entry1, Entry<MemorySegment> entry2) {
        return comparator(entry1.key(), entry2.key());
    }

    public NotOnlyInMemoryDao(Config config) {
        ssTablesStorage = new SSTablesStorage(config);
        remainingMemory = new AtomicLong(config.flushThresholdBytes());
        this.config = config;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (isClosed) {
            throw new DaoClosedException();
        }

        PeekingIterator<Entry<MemorySegment>> rangeIterator = rangeIterator(from, to);
        return new SkipTombstoneIterator(rangeIterator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (isClosed) {
            throw new DaoClosedException();
        }

        Entry<MemorySegment> entry = entries.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }

        entry = flushingEntries.get(key);
        if (entry != null) {
            return entry.value() == null ? null : entry;
        }

        Iterator<Entry<MemorySegment>> iterator = rangeIterator(key, null);

        if (iterator.hasNext()) {
            Entry<MemorySegment> result = iterator.next();
            if (comparator(key, result.key()) == 0) {
                return result.value() == null ? null : result;
            }
        }

        return null;
    }

    private PeekingIterator<Entry<MemorySegment>> rangeIterator(MemorySegment from, MemorySegment to) {
        List<PeekingIterator<Entry<MemorySegment>>> allIterators = new ArrayList<>();

        Iterator<Entry<MemorySegment>> memoryIterator = memoryIterator(from, to);
        allIterators.add(new PeekingIteratorImpl<>(memoryIterator));
        allIterators.add(new PeekingIteratorImpl<>(ssTablesStorage.iteratorsAll(from, to), SS_TABLE_PRIORITY));

        return new PeekingIteratorImpl<>(MergeIterator.merge(allIterators, NotOnlyInMemoryDao::entryComparator));
    }

    private PeekingIterator<Entry<MemorySegment>> iteratorForCompaction() {
        return new PeekingIteratorImpl<>(ssTablesStorage.iteratorsAll(null, null));
    }

    public Iterator<Entry<MemorySegment>> memoryIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return MergeIterator.merge(
                    List.of(new PeekingIteratorImpl<>(entries.values().iterator(), 0),
                            new PeekingIteratorImpl<>(flushingEntries.values().iterator(), 1)),
                    NotOnlyInMemoryDao::entryComparator);

        } else if (from == null) {
            return MergeIterator.merge(
                    List.of(new PeekingIteratorImpl<>(entries.headMap(to).values().iterator(), 0),
                            new PeekingIteratorImpl<>(flushingEntries.headMap(to).values().iterator(), 1)),
                    NotOnlyInMemoryDao::entryComparator);

        } else if (to == null) {
            return MergeIterator.merge(List.of(
                            new PeekingIteratorImpl<>(entries.tailMap(from).values().iterator(), 0),
                            new PeekingIteratorImpl<>(flushingEntries.tailMap(from).values().iterator(), 1)),
                    NotOnlyInMemoryDao::entryComparator);

        } else {
            return MergeIterator.merge(List.of(
                            new PeekingIteratorImpl<>(entries.subMap(from, to).values().iterator(), 0),
                            new PeekingIteratorImpl<>(flushingEntries.subMap(from, to).values().iterator(), 1)),
                    NotOnlyInMemoryDao::entryComparator);
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (isClosed) {
            throw new DaoClosedException();
        }

        if (remainingMemory.get() <= 0) {
            try {
                flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        synchronized (this) {
            entries.put(entry.key(), entry);
            remainingMemory.addAndGet(-SSTableUtils.entryByteSize(entry));
        }
    }

    @Override
    public void flush() throws IOException {
        if (isClosed) {
            throw new DaoIsFlushingException();
        }

        if (isFlushing) {
            throw new DaoClosedException();
        }

        isFlushing = true;
        synchronized (this) {
            flushingEntries = new ConcurrentSkipListMap<>(entries);
            entries = new ConcurrentSkipListMap<>(NotOnlyInMemoryDao::comparator);
            remainingMemory = new AtomicLong(config.flushThresholdBytes());
        }

        threadExecutor.execute(this::performFlush);
    }

    private void performFlush() {
        try {
            ssTablesStorage.write(flushingEntries);
            flushingEntries = new ConcurrentSkipListMap<>(NotOnlyInMemoryDao::comparator);
        } catch (IOException e) {
            throw new RuntimeException("Can't flush", e);
        }

        isFlushing = false;
    }

    @Override
    public void compact() throws IOException {
        if (isClosed) {
            throw new DaoClosedException();
        }

        threadExecutor.execute(this::performCompact);
    }

    private synchronized void performCompact() {
        if (isClosed) {
            return;
        }

        Iterator<Entry<MemorySegment>> iterator = new SkipTombstoneIterator(iteratorForCompaction());
        if (!iterator.hasNext()) {
            return;
        }

        long sizeForCompaction = 0;
        long entryCount = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            sizeForCompaction += SSTableUtils.entryByteSize(entry);
            entryCount++;
        }
        sizeForCompaction += 2L * Long.BYTES * entryCount;
        sizeForCompaction += Long.BYTES + Long.BYTES * entryCount; //for metadata (header + key offsets)

        iterator = new SkipTombstoneIterator(iteratorForCompaction());
        try {
            ssTablesStorage.compact(iterator, sizeForCompaction, entryCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (isClosed) {
            return;
        }

        isClosed = true;

        if (!entries.isEmpty()) {
            flushingEntries = new ConcurrentSkipListMap<>(entries);
            entries = new ConcurrentSkipListMap<>(NotOnlyInMemoryDao::comparator);
            remainingMemory = new AtomicLong(config.flushThresholdBytes());
            ssTablesStorage.write(flushingEntries);
            flushingEntries = new ConcurrentSkipListMap<>(NotOnlyInMemoryDao::comparator);
        }

        threadExecutor.shutdown();
    }
}
