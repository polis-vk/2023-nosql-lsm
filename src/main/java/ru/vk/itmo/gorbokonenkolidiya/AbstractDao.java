package ru.vk.itmo.gorbokonenkolidiya;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public abstract class AbstractDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    protected final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;

    protected AbstractDao() {
        memTable = new ConcurrentSkipListMap<>(AbstractDao::compare);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memTable.values().iterator();
        }
        if (from != null) {
            if (to != null) {
                return memTable.subMap(from, to).values().iterator();
            }
            return memTable.tailMap(from, true).values().iterator();
        }

        return memTable.headMap(to, false).values().iterator();
    }

    @Override
    public abstract Entry<MemorySegment> get(MemorySegment key);

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    protected static int compare(MemorySegment first, MemorySegment second) {
        long offset = MemorySegment.mismatch(first, 0, first.byteSize(), second, 0, second.byteSize());

        // Equal segments
        if (offset == -1) {
            return 0;
        }

        if (offset == first.byteSize()) {
            return -1;
        } else if (offset == second.byteSize()) {
            return 1;
        }

        var firstByteToCompare = first.get(ValueLayout.JAVA_BYTE, offset);
        var secondByteToCompare = second.get(ValueLayout.JAVA_BYTE, offset);

        return Byte.compare(firstByteToCompare, secondByteToCompare);
    }
}
