package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.PeekTableIterator;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable implements TableMap<MemorySegment, MemorySegment>, Iterable<Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> entriesMap;
    private long byteSize;

    public MemTable(Comparator<MemorySegment> comparator) {
        this.entriesMap = new ConcurrentSkipListMap<>(comparator);
        this.byteSize = 0L;
    }

    public void upsert(Entry<MemorySegment> entry) {
        synchronized (entriesMap) {
            byteSize += (entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize()));
        }
        entriesMap.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> getEntry(MemorySegment key) {
        return entriesMap.get(key);
    }

    @Override
    public MemorySegment ceilKey(MemorySegment key) {
        return entriesMap.ceilingKey(key);
    }

    @Override
    public int size() {
        return entriesMap.size();
    }

    @Override
    public PeekTableIterator<MemorySegment> keyIterator(MemorySegment from, MemorySegment to) {
        return new PeekTableIterator<>(getSubMap(from, to).keySet().iterator(), Integer.MAX_VALUE);
    }

    @Override
    public boolean contains(MemorySegment key) {
        return entriesMap.containsKey(key);
    }

    public long byteSize() {
        return this.byteSize;
    }

    /**
     * Return metadata length of SSTable file.
     * Metadata contains amount of entries in sst, offsets and size of keys.
     * It has the following format: <var>size keyOff1:valOff1 keyOff2:valOff2 ...
     * keyOff_n:valOff_n keyOff_n+1:valOff_n+1</var>
     * without any : and spaces.
     */
    public long getMetaDataSize() {
        return 2L * (entriesMap.size() + 1) * Long.BYTES + Long.BYTES;
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
    public void close() {
        entriesMap.clear();
        byteSize = 0L;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return entriesMap.values().iterator();
    }

    @Override
    public boolean isTombstone(Entry<MemorySegment> entry) {
        return entry.value() == null && contains(entry.key());
    }
}
