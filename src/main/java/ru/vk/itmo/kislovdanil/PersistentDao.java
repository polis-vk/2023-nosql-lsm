package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kislovdanil.exceptions.DBException;
import ru.vk.itmo.kislovdanil.exceptions.OverloadException;
import ru.vk.itmo.kislovdanil.iterators.DatabaseIterator;
import ru.vk.itmo.kislovdanil.iterators.MergeIterator;
import ru.vk.itmo.kislovdanil.sstable.SSTable;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>>, Iterable<Entry<MemorySegment>> {

    public static final MemorySegment DELETED_VALUE = null;
    private final Config config;
    private final List<SSTable> tables = new CopyOnWriteArrayList<>();
    private final Comparator<MemorySegment> comparator = new MemSegComparator();
    private volatile MemTable memTable = new MemTable(comparator);

    // Temporary storage in case of main storage flushing (Read only)
    private volatile NavigableMap<MemorySegment, Entry<MemorySegment>> additionalStorage =
            memTable.storage;

    // In case of additional table overload while main table is flushing
    private volatile boolean isFlushing;

    private final AtomicLong nextId = new AtomicLong();
    private final ExecutorService commonExecutorService = Executors.newSingleThreadExecutor();
    private final AtomicLong memTableByteSize = new AtomicLong(0);

    private long getMaxTablesId(Iterable<SSTable> tableIterable) {
        long curMaxId = -1;
        for (SSTable table : tableIterable) {
            curMaxId = Math.max(curMaxId, table.getTableId());
        }
        return curMaxId;
    }

    public PersistentDao(Config config) throws IOException {
        this.config = config;
        File basePathDirectory = new File(config.basePath().toString());
        String[] ssTablesIds = basePathDirectory.list();
        if (ssTablesIds == null) return;
        for (String tableID : ssTablesIds) {
            // SSTable constructor without entries iterator reads table data from disk if it exists
            tables.add(new SSTable(config.basePath(), comparator, Long.parseLong(tableID)));
        }
        nextId.set(getMaxTablesId(tables) + 1);
        tables.sort(SSTable::compareTo);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<DatabaseIterator> iterators = new ArrayList<>(tables.size() + 2);
        for (SSTable table : tables) {
            iterators.add(table.getRange(from, to));
        }
        iterators.add(new MemTableIterator(from, to, memTable.storage, Long.MAX_VALUE));
        iterators.add(new MemTableIterator(from, to, additionalStorage, Long.MAX_VALUE - 1));
        return new MergeIterator(iterators, comparator);
    }

    private static Entry<MemorySegment> wrapEntryIfDeleted(Entry<MemorySegment> entry) {
        if (entry.value() == DELETED_VALUE) return null;
        return entry;
    }

    private long getNextId() {
        return nextId.getAndIncrement();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> ans = memTable.storage.get(key);
        if (ans != null) return wrapEntryIfDeleted(ans);
        ans = additionalStorage.get(key);
        if (ans != null) return wrapEntryIfDeleted(ans);
        try {
            for (SSTable table : tables.reversed()) {
                ans = table.find(key);
                if (ans != null) {
                    return wrapEntryIfDeleted(ans);
                }
            }
        } catch (IOException e) {
            throw new DBException(e);
        }
        return null;
    }

    private long getEntryByteSize(Entry<MemorySegment> entry) {
        long entryByteSize = entry.key().byteSize();
        if (entry.value() != null) {
            entryByteSize += entry.value().byteSize();
        }
        return entryByteSize;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        long entryByteSize = getEntryByteSize(entry);
        long tableSize = memTableByteSize.get();
        while (tableSize > config.flushThresholdBytes()) {
            if (isFlushing) {
                throw new OverloadException(entry);
            } else if (memTableByteSize.compareAndSet(tableSize, 0)) {
                flush();
                break;
            } else {
                tableSize = memTableByteSize.get();
            }
        }
        memTableByteSize.addAndGet(entryByteSize);
        Entry<MemorySegment> previousEntry = memTable.put(entry);
        if (previousEntry != null) {
            memTableByteSize.addAndGet(-getEntryByteSize(previousEntry));
        }
    }

    private void makeFlush() throws IOException {
        if (memTable.storage.isEmpty()) {
            return;
        }
        MemTable currentMemTable = memTable;
        additionalStorage = currentMemTable.storage;
        memTable = new MemTable(comparator);
        // Necessary to get snapshot without data loss
        currentMemTable.lock.writeLock().lock();
        try {
            // SSTable constructor with entries iterator writes MemTable data on disk deleting old data if it exists
            tables.add(new SSTable(config.basePath(), comparator,
                    getNextId(), additionalStorage.values().iterator()));
        } finally {
            currentMemTable.lock.writeLock().unlock();
        }
    }

    @Override
    public void flush() {
        commonExecutorService.execute(
                () -> {
                    try {
                        isFlushing = true;
                        makeFlush();
                    } catch (IOException e) {
                        throw new DBException(e);
                    } finally {
                        isFlushing = false;
                    }
                });
    }

    private void closeExecutorService(ExecutorService executorService) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        closeExecutorService(commonExecutorService);
    }

    private void makeCompaction() throws IOException {
        if (tables.size() <= 1) return;
        SSTable[] curTables = new SSTable[tables.size()];
        long compactedTableId = getNextId();
        tables.toArray(curTables);
        SSTable compactedTable = new SSTable(config.basePath(), comparator, compactedTableId,
                new MergeIterator(Arrays.asList(curTables), comparator));
        tables.add(compactedTable);
        for (SSTable table : curTables) {
            tables.remove(table);
            table.deleteFromDisk(compactedTable);
        }
    }

    @Override
    public void compact() {
        commonExecutorService.execute(
                () -> {
                    try {
                        makeCompaction();
                    } catch (IOException e) {
                        throw new DBException(e);
                    }
                });
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return get(null, null);
    }

    private static class MemSegComparator implements Comparator<MemorySegment> {
        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {
            long mismatch = o1.mismatch(o2);
            if (mismatch == -1) {
                return 0;
            }
            if (mismatch == Math.min(o1.byteSize(), o2.byteSize())) {
                return Long.compare(o1.byteSize(), o2.byteSize());
            }
            return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, mismatch), o2.get(ValueLayout.JAVA_BYTE, mismatch));
        }
    }

    private static class MemTableIterator implements DatabaseIterator {
        private final Iterator<Entry<MemorySegment>> innerIter;
        private final long priority;

        public MemTableIterator(MemorySegment from, MemorySegment to,
                                NavigableMap<MemorySegment, Entry<MemorySegment>> memTable,
                                long priority) {
            this.priority = priority;
            if (from == null && to == null) {
                innerIter = memTable.values().iterator();
            } else if (from != null && to == null) {
                innerIter = memTable.tailMap(from).values().iterator();
            } else if (from == null) {
                innerIter = memTable.headMap(to).values().iterator();
            } else {
                innerIter = memTable.subMap(from, to).values().iterator();
            }
        }

        @Override
        public long getPriority() {
            return priority;
        }

        @Override
        public boolean hasNext() {
            return innerIter.hasNext();
        }

        @Override
        public Entry<MemorySegment> next() {
            return innerIter.next();
        }
    }
}
