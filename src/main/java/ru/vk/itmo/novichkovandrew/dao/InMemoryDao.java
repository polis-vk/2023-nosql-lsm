package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    protected final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> entriesMap;
    protected final Comparator<MemorySegment> comparator = (first, second) -> {
        if (first == null || second == null) return -1;
        long missIndex = first.mismatch(second);
        if (missIndex == first.byteSize()) {
            return -1;
        }
        if (missIndex == second.byteSize()) {
            return 1;
        }
        return missIndex == -1 ? 0 : Byte.compare(
                first.getAtIndex(ValueLayout.JAVA_BYTE, missIndex),
                second.getAtIndex(ValueLayout.JAVA_BYTE, missIndex)
        );
    };

    protected final AtomicLong tableByteSize = new AtomicLong(0);

    public InMemoryDao() {
        this.entriesMap = new ConcurrentSkipListMap<>(comparator);
    }

    private NavigableMap<MemorySegment, Entry<MemorySegment>> getSubMap(MemorySegment from, MemorySegment to) {
        if (from != null && to != null) {
            return entriesMap.subMap(from, to);
        }
        if (from != null) {
            return entriesMap.tailMap(from, true);
        }
        if (to != null) {
            return entriesMap.headMap(to, false);
        }
        return entriesMap;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getSubMap(from, to).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return entriesMap.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        tableByteSize.addAndGet(entry.key().byteSize() + entry.value().byteSize());
        entriesMap.put(entry.key(), entry);
    }

    protected long getMetaDataSize() {
        return 2L * (entriesMap.size() + 1) * Long.BYTES + Long.BYTES;
    }
}
