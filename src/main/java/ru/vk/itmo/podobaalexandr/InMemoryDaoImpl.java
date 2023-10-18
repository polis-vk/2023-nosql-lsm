package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Comparator<MemorySegment> comparator = MemorySegmentUtils::compare;

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentEntryMap
            = new ConcurrentSkipListMap<>(comparator);

    private final SSTableWriter ssTableWriter;

    private final SSTableReader ssTableReader;

    public InMemoryDaoImpl() {
       this(new Config(Path.of("standard")));
    }

    public InMemoryDaoImpl(Config config) {
        ssTableReader = new SSTableReader(config.basePath());
        ssTableWriter = new SSTableWriter(config.basePath(), ssTableReader.size());
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> innerMap = memorySegmentEntryMap;
        Collection<Entry<MemorySegment>> entries;

        if (from == null && to == null) {
            entries = ssTableReader.allPages(innerMap);
        } else if (from != null && to != null) {
            innerMap = innerMap.subMap(from, to);
            entries = ssTableReader.allPagesFromTo(from, to, innerMap);
        } else if (from != null) {
            innerMap = innerMap.tailMap(from);
            entries = ssTableReader.allPagesFrom(from, innerMap);
        } else {
            innerMap = innerMap.headMap(to);
            entries = ssTableReader.allPagesTo(to, innerMap);
        }

        entries.removeIf(it -> it.value() == null);
        return entries.iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memorySegmentEntryMap.put(entry.key(), entry);
    }

    @Override
    public void close() {
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

}
