package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    protected final MemTable memTable;
    protected final Comparator<MemorySegment> comparator = (first, second) -> {
        if (first == null || second == null) return -1;
        long missIndex = first.mismatch(second);
        if (missIndex == first.byteSize()) {
            return -1;
        }
        if (missIndex == second.byteSize()) {
            return 1;
        }
        return missIndex == -1 ? 0 : Byte.compare(
                first.getAtIndex(ValueLayout.JAVA_BYTE, missIndex),
                second.getAtIndex(ValueLayout.JAVA_BYTE, missIndex)
        );
    };

    public InMemoryDao() {
        this.memTable = new MemTable(comparator);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return memTable.get(from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return memTable.get(key);
    }

    @Override
    public synchronized void upsert(Entry<MemorySegment> entry) {
        memTable.add(entry);
    }

    @Override
    public void close() throws IOException {
        memTable.close();
    }
}
