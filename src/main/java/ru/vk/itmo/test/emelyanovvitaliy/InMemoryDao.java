package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = Comparator.comparing(MemorySegment::asByteBuffer);
    private final ConcurrentSkipListMap<MemorySegment, MemorySegment> mappings = new ConcurrentSkipListMap<>(
            comparator
    );

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return new BaseEntry<>(key, mappings.get(key));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (mappings.isEmpty()) {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Entry<MemorySegment> next() {
                    throw new NoSuchElementException();
                }
            };
        }
        boolean includeLast = false;
        if (from == null) {
            from = mappings.firstKey();
        }
        if (to == null) {
            to = mappings.lastKey();
            includeLast = true;
        }
        return privateGet(from, to, includeLast);
    }


    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mappings.put(entry.key(), entry.value());
    }

    private Iterator<Entry<MemorySegment>> privateGet(MemorySegment from, MemorySegment to, boolean includeLast) {
        Iterator<Map.Entry<MemorySegment, MemorySegment>> it = mappings.subMap(from, true, to, includeLast)
                .sequencedEntrySet().iterator();
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Entry<MemorySegment> next() {
                Map.Entry<MemorySegment, MemorySegment> entry = it.next();
                return new BaseEntry<>(entry.getKey(), entry.getValue());
            }
        };
    }

}
