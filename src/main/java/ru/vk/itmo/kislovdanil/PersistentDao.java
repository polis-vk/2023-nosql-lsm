package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kislovdanil.exceptions.DBException;
import ru.vk.itmo.kislovdanil.iterators.DatabaseIterator;
import ru.vk.itmo.kislovdanil.iterators.MergeIterator;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>>, Iterable<Entry<MemorySegment>> {

    public static final MemorySegment DELETED_VALUE = null;
    private final Config config;
    private final List<SSTable> tables = new CopyOnWriteArrayList<>();
    private final Comparator<MemorySegment> comparator = new MemSegComparator();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(comparator);

    private final AtomicLong nextId = new AtomicLong();
    private final AtomicBoolean isCompacting = new AtomicBoolean(false);
    // private final Executor flushExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService compactionExecutor = Executors.newSingleThreadExecutor();

    public PersistentDao(Config config) throws IOException {
        this.config = config;
        File basePathDirectory = new File(config.basePath().toString());
        String[] ssTablesIds = basePathDirectory.list();
        if (ssTablesIds == null) return;
        long maxId = 0;
        for (String tableID : ssTablesIds) {
            // SSTable constructor with rewrite=false reads table data from disk if it exists
            long tableId = Long.parseLong(tableID);
            maxId = Math.max(maxId, tableId);
            tables.add(new SSTable(config.basePath(), comparator, Long.parseLong(tableID),
                    storage.values().iterator(), false));
        }
        nextId.set(maxId + 1);
        tables.sort(SSTable::compareTo);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<DatabaseIterator> iterators = new ArrayList<>(tables.size() + 1);
        for (SSTable table : tables) {
            iterators.add(table.getRange(from, to));
        }
        iterators.add(new MemTableIterator(from, to));
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
        Entry<MemorySegment> ans = storage.get(key);
        if (ans != null) return wrapEntryIfDeleted(ans);
        try {
            for (SSTable table : tables.reversed()) {
                Entry<MemorySegment> data = table.find(key);
                if (data != null) {
                    return wrapEntryIfDeleted(data);
                }
            }
        } catch (IOException e) {
            throw new DBException(e);
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (!storage.isEmpty()) {
            // SSTable constructor with rewrite=true writes MemTable data on disk deleting old data if it exists
            tables.add(new SSTable(config.basePath(), comparator,
                    getNextId(), storage.values().iterator(), true));
            storage.clear();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        compactionExecutor.shutdown();
        try {
            if (!compactionExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void makeCompaction() throws IOException {
        if (!tables.isEmpty()) {
            SSTable[] curTables = new SSTable[tables.size()];
            tables.toArray(curTables);
            SSTable compactedTable = new SSTable(config.basePath(), comparator, getNextId(),
                    new MergeIterator(Arrays.asList(curTables), comparator), true);
            tables.add(compactedTable);
            for (SSTable table : curTables) {
                tables.remove(table);
                table.deleteFromDisk(compactedTable);
            }
        }
    }

    @Override
    public void compact() {
        if (isCompacting.compareAndSet(false, true)) {
            compactionExecutor.execute(() -> {
                try {
                    makeCompaction();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            isCompacting.set(false);
        }
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

    private class MemTableIterator implements DatabaseIterator {
        Iterator<Entry<MemorySegment>> innerIter;

        public MemTableIterator(MemorySegment from, MemorySegment to) {
            if (from == null && to == null) {
                innerIter = storage.values().iterator();
            } else if (from != null && to == null) {
                innerIter = storage.tailMap(from).values().iterator();
            } else if (from == null) {
                innerIter = storage.headMap(to).values().iterator();
            } else {
                innerIter = storage.subMap(from, to).values().iterator();
            }
        }

        @Override
        public long getPriority() {
            return Long.MAX_VALUE;
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
