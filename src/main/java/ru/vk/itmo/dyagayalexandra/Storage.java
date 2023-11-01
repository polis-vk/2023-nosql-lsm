package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Storage implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> dataStorage;
    private final FileManager fileManager;

    public Storage() {
        dataStorage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        fileManager = null;
    }

    public Storage(Config config) {
        dataStorage = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        fileManager = new FileManager(config);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Iterator<Entry<MemorySegment>> memoryIterator;
        if (from == null && to == null) {
            memoryIterator = dataStorage.values().iterator();
        } else if (from == null) {
            memoryIterator = dataStorage.headMap(to).values().iterator();
        } else if (to == null) {
            memoryIterator = dataStorage.tailMap(from).values().iterator();
        } else {
            memoryIterator = dataStorage.subMap(from, to).values().iterator();
        }

        Iterator<Entry<MemorySegment>> iterators = null;
        if (fileManager != null) {
            iterators = fileManager.createIterators(from, to);
        }

        if (iterators == null) {
            return memoryIterator;
        } else {
            Iterator<Entry<MemorySegment>> mergeIterator = MergedIterator.createMergedIterator(
                    List.of(
                            new PeekingIterator(0, memoryIterator),
                            new PeekingIterator(1, iterators)
                    ),
                    new EntryKeyComparator()
            );

            return new SkipNullIterator(new PeekingIterator(0, mergeIterator));
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (fileManager == null) {
            return dataStorage.get(key);
        } else {
            Iterator<Entry<MemorySegment>> iterator = get(key, null);
            if (!iterator.hasNext()) {
                return null;
            }

            Entry<MemorySegment> next = iterator.next();
            if (new MemorySegmentComparator().compare(key, next.key()) == 0) {
                return next;
            }
            return null;
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        dataStorage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        if (dataStorage.isEmpty() || fileManager == null) {
            return;
        }

        fileManager.flush(dataStorage);
    }

    @Override
    public void compact() {
        Iterator<Entry<MemorySegment>> iterator = get(null, null);
        if (!iterator.hasNext()) {
            return;
        }

        if (fileManager == null) {
            return;
        }

        try {
            fileManager.performCompact(iterator, !dataStorage.isEmpty());
        } catch (IOException e) {
            throw new UncheckedIOException("Error when executing the compact.", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (fileManager == null) {
            return;
        }

        flush();
        fileManager.closeArena();
        dataStorage.clear();
    }
}
