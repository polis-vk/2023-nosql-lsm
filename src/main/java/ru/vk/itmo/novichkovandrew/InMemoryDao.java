package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    final ConcurrentSkipListSet<Entry<MemorySegment>> data;

    public InMemoryDao() {
        this.data = new ConcurrentSkipListSet<>((o1, o2) -> compareMemorySegment(o1.key(), o2.key()));
    }

    private NavigableSet<Entry<MemorySegment>> getSubMap(Entry<MemorySegment> from, Entry<MemorySegment> to) {
        if (from.key() != null && to.key() != null) {
            return data.subSet(from, to);
        } else if (from.key() != null) {
            return data.tailSet(from, true);
        } else if (to.key() != null) {
            return data.headSet(to, false);
        }
        return data;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Entry<MemorySegment> dummyFrom = new BaseEntry<>(from, null);
        Entry<MemorySegment> dummyTo = new BaseEntry<>(to, null);
        return getSubMap(dummyFrom, dummyTo).iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> dummyEntry = new BaseEntry<>(key, null);
        Entry<MemorySegment> value = data.ceiling(dummyEntry);
        return value == null || value.key().mismatch(key) != -1 ? null : value;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        data.add(entry);
    }

    private int compareMemorySegment(MemorySegment first, MemorySegment second) {
        if (first == null || second == null) return -1;
        if (first.byteSize() != second.byteSize()) {
            return Long.compare(first.byteSize(), second.byteSize());
        }
        long missIndex = first.mismatch(second);
        return missIndex == -1 ? 0 : Byte.compare(
                first.getAtIndex(ValueLayout.JAVA_BYTE, missIndex),
                second.getAtIndex(ValueLayout.JAVA_BYTE, missIndex)
        );
    }
}
