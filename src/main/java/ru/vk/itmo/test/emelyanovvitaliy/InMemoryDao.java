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
    private final Comparator<MemorySegment> comparator = (o1, o2) -> {
        if (o1 == o2) {
            return 0;
        }
        if (o1.byteSize() != o2.byteSize()) {
            return Long.compare(o1.byteSize(), o2.byteSize());
        }
        return o1.asByteBuffer().compareTo(o2.asByteBuffer());
    };
    private final ConcurrentSkipListMap<MemorySegment, MemorySegment> mappings = new ConcurrentSkipListMap<>(
            comparator
    );

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return new BaseEntry<>(key, mappings.get(key));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        MemorySegment acutalFrom = from;
        MemorySegment acutalTo = to;
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
        if (acutalFrom == null) {
            acutalFrom = mappings.firstKey();
        }
        if (acutalTo == null) {
            acutalTo = mappings.lastKey();
            includeLast = true;
        }
        return privateGet(acutalFrom, acutalTo, includeLast);
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
