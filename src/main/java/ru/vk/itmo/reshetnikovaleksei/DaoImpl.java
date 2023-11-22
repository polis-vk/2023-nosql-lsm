package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.iterators.MergeIterator;
import ru.vk.itmo.reshetnikovaleksei.iterators.PeekingIterator;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable;
    private final SSTableManager ssTableManager;

    public DaoImpl(Config config) throws IOException {
        this.memoryTable = new ConcurrentSkipListMap<>(MemorySegmentComparator.getInstance());
        this.ssTableManager = new SSTableManager(config);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = allIterators(key, null);

        if (iterator.hasNext()) {
            Entry<MemorySegment> result = iterator.next();
            if (MemorySegmentComparator.getInstance().compare(key, result.key()) == 0) {
                return result.value() == null ? null : result;
            }
        }

        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return allIterators(from, to);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memoryTable.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (!memoryTable.isEmpty()) {
            ssTableManager.save(memoryTable.values());
            memoryTable.clear();
        }

        ssTableManager.close();
    }

    @Override
    public void compact() throws IOException {
        ssTableManager.compact(() -> get(null, null));
        memoryTable.clear();
    }

    private Iterator<Entry<MemorySegment>> allIterators(MemorySegment from, MemorySegment to) {
        List<PeekingIterator> iterators = new ArrayList<>();

        Iterator<Entry<MemorySegment>> memoryIterator = memoryIterator(from, to);
        Iterator<Entry<MemorySegment>> filesIterator = ssTableManager.get(from, to);

        iterators.add(new PeekingIterator(memoryIterator, 1));
        iterators.add(new PeekingIterator(filesIterator, 0));

        return new PeekingIterator(
                MergeIterator.merge(
                        iterators,
                        MemorySegmentComparator.getInstance()
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
