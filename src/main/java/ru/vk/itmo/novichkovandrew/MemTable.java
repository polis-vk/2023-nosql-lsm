package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.lang.foreign.MemorySegment;
import java.util.AbstractCollection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable extends AbstractCollection<Entry<MemorySegment>> implements Closeable {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> entriesMap;
    private long byteSize;

    private final int capacity;

    public MemTable(Comparator<MemorySegment> comparator) {
        this(comparator, 0);
    }

    /**
     * Constructor for MemTable with initial capacity.
     * If size >= capacity, that MemTable move into memory.
     * Make it for future.
     */
    public MemTable(Comparator<MemorySegment> comparator, int capacity) {
        this.entriesMap = new ConcurrentSkipListMap<>(comparator);
        this.byteSize = 0;
        this.capacity = capacity;
    }

    @Override
    public boolean add(Entry<MemorySegment> entry) {
        byteSize += (entry.key().byteSize() + entry.value().byteSize());
        entriesMap.put(entry.key(), entry);
        return true;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return this.entriesMap.values().iterator();
    }

    @Override
    public int size() {
        return entriesMap.size();
    }

    public long byteSize() {
        return this.byteSize;
    }

    /**
     * Return metadata length of SSTable file.
     * Metadata contains amount of entries in sst, offsets and size of keys.
     * It has the following format: <var>off1:key_sz1 off2:key_sz2 ... offn:key_szn off_n+1:key_sz_n+1 \n</var>
     */
    public long getOffsetsDataSize() {
        return 2L * (entriesMap.size() + 1) * (Character.BYTES + Long.BYTES);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return entriesMap.get(key);
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getSubMap(from, to).values().iterator();
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
        byteSize = 0;
    }
}
