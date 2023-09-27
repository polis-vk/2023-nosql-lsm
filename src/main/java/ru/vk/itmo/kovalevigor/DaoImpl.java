package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage;

    public static final Comparator<MemorySegment> COMPARATOR = (lhs, rhs) -> {
        final long mismatch = lhs.mismatch(rhs);
        final long lhsSize = lhs.byteSize();
        final long rhsSize = rhs.byteSize();
        final long minSize = Math.min(lhsSize, rhsSize);
        if (mismatch == -1) {
            return 0;
        } else if (minSize == mismatch) {
            return Long.compare(lhsSize, rhsSize);
        }
        return Byte.compare(getByte(lhs, mismatch), getByte(rhs, mismatch));
    };

    public DaoImpl() {
        storage = new ConcurrentSkipListMap<>(COMPARATOR);
    }

    private static <T> Iterator<T> getValuesIterator(final ConcurrentNavigableMap<?, T> map) {
        return map.values().iterator();
    }

    private static byte getByte(final MemorySegment memorySegment, final long offset) {
        return memorySegment.get(ValueLayout.JAVA_BYTE, offset);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        if (from == null) {
            if (to == null) {
                return all();
            }
            return allTo(to);
        } else if (to == null) {
            return allFrom(from);
        }
        return getValuesIterator(storage.subMap(from, to));
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        Objects.requireNonNull(entry);
        storage.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> allFrom(final MemorySegment from) {
        Objects.requireNonNull(from);
        return getValuesIterator(storage.tailMap(from));
    }

    @Override
    public Iterator<Entry<MemorySegment>> allTo(final MemorySegment to) {
        Objects.requireNonNull(to);
        return getValuesIterator(storage.headMap(to));
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return getValuesIterator(storage);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        return storage.get(key);
    }
}
