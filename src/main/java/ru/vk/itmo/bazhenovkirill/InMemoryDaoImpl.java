package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> storage = new ConcurrentSkipListMap<>(
            (ms1, ms2) -> {
                long mismatch = ms1.mismatch(ms2);
                if (mismatch == -1) {
                    return 0;
                }
                if (ms2.byteSize() == mismatch) {
                    return 1;
                }
                if (ms1.byteSize() == mismatch) {
                    return -1;
                }
                return Byte.compare(
                        ms1.get(ValueLayout.JAVA_BYTE, mismatch),
                        ms2.get(ValueLayout.JAVA_BYTE, mismatch));
            }
    );

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to != null) {
                return storage.headMap(to).values().iterator();
            }
            return storage.values().iterator();
        }
        if (to == null) {
            return storage.tailMap(from).values().iterator();
        }
        return storage.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

}
