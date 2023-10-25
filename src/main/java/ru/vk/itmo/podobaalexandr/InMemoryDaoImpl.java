package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Comparator<MemorySegment> comparator = MemorySegmentUtils::compare;

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntryMap
            = new ConcurrentSkipListMap<>(comparator);

    private final SSTableWriter ssTableWriter;
    private final SSTableReader ssTableReader;
    private final Compacter compacter;

    public InMemoryDaoImpl() {
       this(new Config(Path.of("standard")));
    }

    public InMemoryDaoImpl(Config config) {
        Path indexFile = config.basePath().resolve("index.idx");
        Path indexTemp = config.basePath().resolve("index.tmp");

        ssTableReader = new SSTableReader(config.basePath(), indexFile, indexTemp);
        ssTableWriter = new SSTableWriter(config.basePath(), indexFile, indexTemp);
        compacter = new Compacter(config.basePath(), indexFile, indexTemp, ssTableReader);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> innerMap = memorySegmentEntryMap;

        if (from == null) {
            if (to != null) {
                innerMap = innerMap.headMap(to);
            }
        } else {
            if (to == null) {
                innerMap = innerMap.tailMap(from);
            } else {
                innerMap = innerMap.subMap(from, to);
            }
        }

        List<IndexedPeekIterator> peekIterators = new ArrayList<>();

        if (!innerMap.isEmpty()) {
            peekIterators.add(new IndexedPeekIterator(innerMap.values().iterator(), 0));
        }

        if (!ssTableReader.isNoneSSTables()) {
            Collection<Iterator<Entry<MemorySegment>>> iteratorsTable = ssTableReader.iterators(from, to);

            long i = 1;
            for (Iterator<Entry<MemorySegment>> iteratorTable : iteratorsTable) {
                peekIterators.add(new IndexedPeekIterator(iteratorTable, i++));
            }
        }

        return new PriorityIterator(peekIterators);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntryMap.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (ssTableReader.isArenaPresented()) {
            if (!ssTableReader.isAlive()) {
                return;
            }

            ssTableReader.close();
        }

        ssTableWriter.save(memorySegmentEntryMap.values());
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (memorySegmentEntryMap.containsKey(key)) {
            Entry<MemorySegment> entry = memorySegmentEntryMap.get(key);
            return entry.value() == null ? null : entry;
        }

        return ssTableReader.get(key);
    }

    /** Creates two equal Iterators:
     * priorityIterator - to read data from whole mapped data
     * priorityIteratorForSize - to find count of entries
     * Throws IOException
     */
    @Override
    public void compact() throws IOException {
        if (ssTableReader.isNoneSSTables()) {
            return;
        }

        List<IndexedPeekIterator> peekIterators = new ArrayList<>();
        List<IndexedPeekIterator> peekIteratorsForSize = new ArrayList<>();

        Collection<Iterator<Entry<MemorySegment>>> iteratorsTable = ssTableReader.iterators(null, null);
        Collection<Iterator<Entry<MemorySegment>>> iteratorsTableForSize = ssTableReader.iterators(null, null);

        long index = 0;
        for (Iterator<Entry<MemorySegment>> iteratorTable: iteratorsTable) {
            peekIterators.add(new IndexedPeekIterator(iteratorTable, index++));
        }

        index = 0;
        for (Iterator<Entry<MemorySegment>> iteratorTable: iteratorsTableForSize) {
            peekIteratorsForSize.add(new IndexedPeekIterator(iteratorTable, index++));
        }

        PriorityIterator priorityIterator = new PriorityIterator(peekIterators);
        PriorityIterator priorityIteratorForSize = new PriorityIterator(peekIteratorsForSize);

        compacter.compact(priorityIterator, priorityIteratorForSize);
    }
}
