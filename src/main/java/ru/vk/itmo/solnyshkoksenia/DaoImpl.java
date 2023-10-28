package ru.vk.itmo.solnyshkoksenia;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class DaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<Entry<MemorySegment>> comparator =
            Comparator.comparing(o -> ByteBuffer.wrap(o.key().toArray(ValueLayout.JAVA_BYTE)));
    private final List<Entry<MemorySegment>> list = new CopyOnWriteArrayList<>();

    @Override
    public synchronized Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        int fromIndex = getIndex(from, 0);
        int toIndex = getIndex(to, list.size());
        return list.subList(fromIndex, toIndex).iterator();
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        int index = Collections.binarySearch(list, entry, comparator);
        if (index >= 0) {
            list.set(index, entry);
        } else {
            list.add(-(index + 1), entry);
        }
    }

    @Override
    public synchronized Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> iterator = get(key, null);
        if (!iterator.hasNext()) {
            return null;
        }
        Entry<MemorySegment> next = iterator.next();

        if (comparator.compare(baseEntry(next.key()), baseEntry(key)) == 0) {
            return next;
        }
        return null;
    }

    private int getIndex(MemorySegment segment, int defaultIndex) {
        if (segment != null && segment.byteSize() != 0) {
            int index = Collections.binarySearch(list, baseEntry(segment), comparator);
            return index < 0 ? -(index + 1) : index;
        }
        return defaultIndex;
    }

    private Entry<MemorySegment> baseEntry(MemorySegment key) {
        return new BaseEntry<>(key, MemorySegment.NULL);
    }
}
