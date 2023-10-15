package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.novichkovandrew.Cell;
import ru.vk.itmo.novichkovandrew.iterator.PeekTableIterator;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable implements TableMap<MemorySegment, MemorySegment>, Iterable<Cell<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Cell<MemorySegment>> entriesMap;
    private long byteSize;

    public MemTable(Comparator<MemorySegment> comparator) {
        this.entriesMap = new ConcurrentSkipListMap<>(comparator);
        this.byteSize = 0L;
    }

    public void upsert(Cell<MemorySegment> cell) {
        synchronized (entriesMap) {
            byteSize += (cell.key().byteSize() + cell.valueSize());
        }
        entriesMap.put(cell.key(), cell);
    }

    @Override
    public Cell<MemorySegment> getCell(MemorySegment key) {
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


    private NavigableMap<MemorySegment, Cell<MemorySegment>> getSubMap(MemorySegment from, MemorySegment to) {
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

    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public Iterator<Cell<MemorySegment>> iterator() {
        return entriesMap.values().iterator();
    }
}
