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

public class NotOnlyInMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final int SS_TABLE_PRIORITY = 1;
    private final SSTablesStorage ssTablesStorage;
    private final SortedMap<MemorySegment, Entry<MemorySegment>> entries =
            new ConcurrentSkipListMap<>(NotOnlyInMemoryDao::comparator);

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
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        PeekingIterator<Entry<MemorySegment>> rangeIterator = rangeIterator(from, to);
        return new SkipTombstoneIterator(rangeIterator);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
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

        if (!entries.isEmpty()) {
            Iterator<Entry<MemorySegment>> memoryIterator = memoryIterator(from, to);
            allIterators.add(new PeekingIteratorImpl<>(memoryIterator));
        }

        allIterators.add(new PeekingIteratorImpl<>(ssTablesStorage.iteratorsAll(from, to), SS_TABLE_PRIORITY));

        return new PeekingIteratorImpl<>(MergeIterator.merge(allIterators, NotOnlyInMemoryDao::entryComparator));
    }

    //Retrieves iterator for memory table and sstables
    private PeekingIterator<Entry<MemorySegment>> iteratorForCompaction() {
        return rangeIterator(null, null);
    }

    public Iterator<Entry<MemorySegment>> memoryIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return entries.values().iterator();
        } else if (from == null) {
            return entries.headMap(to).values().iterator();
        } else if (to == null) {
            return entries.tailMap(from).values().iterator();
        } else {
            return entries.subMap(from, to).values().iterator();
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        entries.put(entry.key(), entry);
    }

    @Override
    public void compact() throws IOException {
        Iterator<Entry<MemorySegment>> iterator = new SkipTombstoneIterator(iteratorForCompaction());

        long sizeForCompaction = 0;
        long entryCount = 0;
        while (iterator.hasNext()) {
            Entry<MemorySegment> entry = iterator.next();
            sizeForCompaction += SSTablesStorage.entryByteSize(entry);
            entryCount++;
        }
        sizeForCompaction += 2L * Long.BYTES * entryCount;
        sizeForCompaction += Long.BYTES + Long.BYTES * entryCount; //for metadata (header + key offsets)

        iterator = new SkipTombstoneIterator(iteratorForCompaction());
        ssTablesStorage.compact(iterator, sizeForCompaction, entryCount);
    }

    @Override
    public void flush() throws IOException {
        Dao.super.flush();
    }

    @Override
    public void close() throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        ssTablesStorage.write(entries);
        entries.clear();
    }
}
