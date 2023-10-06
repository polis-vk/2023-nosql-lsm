package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.kislovdanil.iterators.DatabaseIterator;
import ru.vk.itmo.kislovdanil.iterators.MergeIterator;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Config config;
    private final List<SSTable> tables = new ArrayList<>();
    private final Comparator<MemorySegment> comparator = new MemSegComparator();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage =
            new ConcurrentSkipListMap<>(comparator);

    public PersistentDao(Config config) throws IOException {
        this.config = config;
        File basePathDirectory = new File(config.basePath().toString());
        String[] SSTablesIds = basePathDirectory.list();
        if (SSTablesIds == null) return;
        for (String tableID : SSTablesIds) {
            tables.add(new SSTable(config.basePath(), comparator, Long.parseLong(tableID), storage, false));
        }
        tables.sort(SSTable::compareTo);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<DatabaseIterator> iterators = new ArrayList<>(tables.size() + 1);
        for (SSTable table: tables) {
            iterators.add(table.getRange(from, to));
        }
        iterators.add(new MemTableIterator(from, to));
        return new MergeIterator(iterators, comparator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> ans = storage.get(key);
        if (ans != null) return ans;
        try {
            for (SSTable table : tables) {
                Entry<MemorySegment> data = table.find(key);
                if (data == null) continue;
                return data;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        tables.add(new SSTable(config.basePath(), comparator,
                System.currentTimeMillis(), storage, true));
    }

    @Override
    public void close() throws IOException {
        flush();
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
            if (from == null && to == null) innerIter = storage.values().iterator();
            else if (from != null && to == null) innerIter = storage.tailMap(from).values().iterator();
            else if (from == null) innerIter = storage.headMap(to).values().iterator();
            else innerIter = storage.subMap(from, to).values().iterator();
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
