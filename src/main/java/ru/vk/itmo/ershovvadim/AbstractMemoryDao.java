package ru.vk.itmo.ershovvadim;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

public abstract class AbstractMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    protected final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> db =
            new ConcurrentSkipListMap<>(this::compare);

    public int compare(MemorySegment segment1, MemorySegment segment2) {
        long mismatchOffset = segment1.mismatch(segment2);
        if (mismatchOffset == -1) {
            return 0;
        } else if (mismatchOffset == segment1.byteSize()) {
            return -1;
        } else if (mismatchOffset == segment2.byteSize()) {
            return 1;
        }

        var offsetByte1 = segment1.get(JAVA_BYTE, mismatchOffset);
        var offsetByte2 = segment2.get(JAVA_BYTE, mismatchOffset);
        return Byte.compare(offsetByte1, offsetByte2);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return db.values().iterator();
        } else if (to == null) {
            return db.tailMap(from).values().iterator();
        } else if (from == null) {
            return db.headMap(to).values().iterator();
        }
        return db.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        db.put(entry.key(), entry);
    }

}
