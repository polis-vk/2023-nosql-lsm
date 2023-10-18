package ru.vk.itmo.reshetnikovaleksei;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.iterator.MergeIterator;
import ru.vk.itmo.reshetnikovaleksei.iterator.PeekingIterator;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable;
    private final SSTable ssTable;
    private final Comparator<MemorySegment> comparator;

    public DaoImpl(Config config) throws IOException {
        this.memoryTable = new ConcurrentSkipListMap<>(new MemorySegmentComparator());
        this.ssTable = new SSTable(config);
        this.comparator = new MemorySegmentComparator();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return allIterator(from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = allIterator(key, null);

        if (iterator.hasNext()) {
            Entry<MemorySegment> result = iterator.next();
            if (comparator.compare(key, result.key()) == 0) {
                return result.value() == null ? null : result;
            }
        }

        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memoryTable.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (!memoryTable.isEmpty()) {
            ssTable.save(memoryTable.values());
        }

        ssTable.close();
        memoryTable.clear();
    }

    private Iterator<Entry<MemorySegment>> allIterator(MemorySegment from, MemorySegment to) {
        List<PeekingIterator> iterators = new ArrayList<>();

        Iterator<Entry<MemorySegment>> memoryIterator = memoryIterator(from ,to);
        Iterator<Entry<MemorySegment>> filesIterator = ssTable.allFilesIterator(from, to);

        iterators.add(new PeekingIterator(memoryIterator, 0));
        iterators.add(new PeekingIterator(filesIterator, 0));

        return new PeekingIterator(
                MergeIterator.merge(
                        iterators,
                        comparator
                )
        );
    }

    private Iterator<Entry<MemorySegment>> memoryIterator(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memoryTable.values().iterator();
        }

        if (from == null) {
            return memoryTable.headMap(to).values().iterator();
        }
        if (to == null) {
            return memoryTable.tailMap(from).values().iterator();
        }

        return memoryTable.subMap(from, to).values().iterator();
    }
}
