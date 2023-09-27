package ru.vk.itmo;

import ru.vk.itmo.danilinandrew.InMemoryDao;
import ru.vk.itmo.danilinandrew.MemorySegmentComparator;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;

public interface Dao<D, E extends Entry<D>> extends Closeable {

    /**
     * Returns ordered iterator of entries with keys between from (inclusive) and to (exclusive).
     * @param from lower bound of range (inclusive)
     * @param to upper bound of range (exclusive)
     * @return entries [from;to)
     */
    Iterator<E> get(D from, D to);

    /**
     * Returns entry by key. Note: default implementation is far from optimal.
     * @param key entry`s key
     * @return entry
     */
    default E get(D key) {
        Iterator<E> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }

        E next = iterator.next();
        MemorySegment o1 = next.key() instanceof MemorySegment ? ((MemorySegment) next.key()) : null;
        MemorySegment o2 = key instanceof MemorySegment ? ((MemorySegment) key) : null;

        if (new MemorySegmentComparator().compare(o1, o2) == 0) {
            return next;
        }
        return null;
    }

    default int compare(MemorySegment o1, MemorySegment o2) {
        int offset = 0;

        while (offset < o1.byteSize() && offset < o2.byteSize()) {
            byte byte1 = o1.get(ValueLayout.JAVA_BYTE, offset);
            byte byte2 = o2.get(ValueLayout.JAVA_BYTE, offset);

            int compareRes = Byte.compare(byte1, byte2);

            if (compareRes == 0) {
                offset++;
            } else {
                return compareRes;
            }
        }

        return Long.compare(o1.byteSize(), o2.byteSize());
    }

    /**
     * Returns ordered iterator of all entries with keys from (inclusive).
     * @param from lower bound of range (inclusive)
     * @return entries with key >= from
     */
    default Iterator<E> allFrom(D from) {
        return get(from, null);
    }

    /**
     * Returns ordered iterator of all entries with keys < to.
     * @param to upper bound of range (exclusive)
     * @return entries with key < to
     */
    default Iterator<E> allTo(D to) {
        return get(null, to);
    }

    /**
     * Returns ordered iterator of all entries.
     * @return all entries
     */
    default Iterator<E> all() {
        return get(null, null);
    }

    /**
     * Inserts of replaces entry.
     * @param entry element to upsert
     */
    void upsert(E entry);

    /*
     * Persists data (no-op by default).
     */
    default void flush() throws IOException {
        //by default do nothing
    }

    /*
     * Releases Dao (calls flush by default).
     */
    @Override
    default void close() throws IOException {
        flush();
    }

}
