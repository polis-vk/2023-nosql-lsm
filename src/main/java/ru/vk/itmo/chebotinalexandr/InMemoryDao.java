package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final SSTable sortedStringTable;
    private final SortedMap<MemorySegment, Entry<MemorySegment>> entries =
            new ConcurrentSkipListMap<>(InMemoryDao::comparator);

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

    public InMemoryDao(Config config) {
        sortedStringTable = new SSTable(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return all();
        } else if (from == null) {
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        } else {
            return entries.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(MemorySegment from) {
        return entries.tailMap(from).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(MemorySegment to) {
        return entries.headMap(to).values().iterator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return entries.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry result = entries.get(key);

        if (result == null) {
            result = sortedStringTable.get(key);
        }

        return result;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        entries.put(entry.key(), entry);
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
        sortedStringTable.write(entries);
    }
}
