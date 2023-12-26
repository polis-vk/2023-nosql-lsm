package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable extends AbstractTable implements Iterable<Entry<MemorySegment>> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> entriesMap;
    private final AtomicLong byteSize;

    public MemTable() {
        this.entriesMap = new ConcurrentSkipListMap<>(comparator);
        this.byteSize = new AtomicLong();
    }

    public void upsert(Entry<MemorySegment> entry) {
        byteSize.addAndGet(entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize()));
        entriesMap.put(entry.key(), entry);
    }

    @Override
    public int size() {
        return entriesMap.size();
    }

    @Override
    public TableIterator<MemorySegment> tableIterator(MemorySegment from, boolean fromInclusive,
                                                      MemorySegment to, boolean toInclusive) {

        return new TableIterator<>() {
            final Iterator<Entry<MemorySegment>> it = getSubMap(from, fromInclusive, to, toInclusive)
                    .values().iterator();

            @Override
            public int getTableNumber() {
                return Integer.MAX_VALUE;
            }

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                return it.next();
            }
        };
    }

    public long byteSize() {
        return this.byteSize.get();
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

    private NavigableMap<MemorySegment, Entry<MemorySegment>> getSubMap(MemorySegment from, boolean fromInclusive,
                                                                        MemorySegment to, boolean toInclusive) {
        if (from != null && to != null) {
            return entriesMap.subMap(from, fromInclusive, to, toInclusive);
        }
        if (from != null) {
            return entriesMap.tailMap(from, fromInclusive);
        }
        if (to != null) {
            return entriesMap.headMap(to, toInclusive);
        }
        return entriesMap;
    }

    @Override
    public void close() {
        entriesMap.clear();
        byteSize.set(0);
    }

    public boolean isTombstone(MemorySegment key) {
        return entriesMap.containsKey(key) && entriesMap.get(key).value() == null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return entriesMap.values().iterator();
    }
}
