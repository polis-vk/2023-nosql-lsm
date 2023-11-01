package ru.vk.itmo.novichkovandrew.table;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

public class MemTable implements Table<MemorySegment> {

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> entriesMap;
    private final AtomicLong byteSize;

    public MemTable(Comparator<MemorySegment> comparator) {
        this.entriesMap = new ConcurrentSkipListMap<>(comparator);
        this.byteSize = new AtomicLong();
    }

    public void upsert(Entry<MemorySegment> entry) {
        updateByteSize(entry);
        entriesMap.put(entry.key(), entry);
    }

    private void updateByteSize(Entry<MemorySegment> entry) {
        if (entriesMap.containsKey(entry.key())) {
            var oldValue = entriesMap.get(entry.key()).value();
            byteSize.addAndGet(oldValue == null ? 0 : -oldValue.byteSize());
        } else {
            byteSize.addAndGet(entry.key().byteSize());
        }
        byteSize.addAndGet(entry.value() == null ? 0 : entry.value().byteSize());
    }

    @Override
    public int rows() {
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

    @Override
    public long byteSize() {
        return this.byteSize.get();
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
        clear();
    }

    @Override
    public void clear() {
        entriesMap.clear();
        byteSize.set(0);
    }
}
