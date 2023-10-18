package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Comparator<MemorySegment> comparator = new MyComparator();

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
            entries = ssTableReader.allPagesFromTo(innerMap, from, to);
        } else if (from != null) {
            innerMap = innerMap.tailMap(from);
            entries = ssTableReader.allPagesFrom(innerMap, from);
        } else {
            innerMap = innerMap.headMap(to);
            entries = ssTableReader.allPagesTo(innerMap, to);
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

    private static class MyComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment o1, MemorySegment o2) {

            int sizeDiff = Long.compare(o1.byteSize(), o2.byteSize());

            if (o1.byteSize() == 0 || o2.byteSize() == 0) {
                return sizeDiff;
            }

            long mismatch = o1.mismatch(o2);

            if (mismatch == o1.byteSize() || mismatch == o2.byteSize()) {
                return sizeDiff;
            }

            return mismatch == -1
                    ? 0
                    : Byte.compare(o1.get(ValueLayout.JAVA_BYTE, mismatch), o2.get(ValueLayout.JAVA_BYTE, mismatch));
        }
    }

}
