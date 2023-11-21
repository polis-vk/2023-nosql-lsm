package ru.vk.itmo.at;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;

public class ATDao implements Dao<MemorySegment, BaseEntry<MemorySegment>> {
    private final MemorySegmentComparator comparator = new MemorySegmentComparator();
    private final NavigableMap<MemorySegment, BaseEntry<MemorySegment>> cache = new ConcurrentSkipListMap<>(comparator);
    private final ISSTable sstable;
    private final String sstableName;

    public ATDao(Config config) throws IOException {
        sstableName = config.basePath() + "/data.dat";
        sstable = SSTable.load(sstableName, comparator);
    }


    @Override
    public Iterator<BaseEntry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to == null) {
                return cache.values().iterator();
            }
            return cache.headMap(to).values().iterator();
        }
        return cache.tailMap(from).headMap(to).values().iterator();
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        BaseEntry<MemorySegment> res = cache.get(key);
        if (res != null) {
            return res;
        }
        return sstable.get(key);
    }

    @Override
    public void upsert(BaseEntry<MemorySegment> entry) {
        cache.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        try {
            sstable.close();
        } finally {
            SSTable.save(sstableName, cache.values());
        }
    }
}
