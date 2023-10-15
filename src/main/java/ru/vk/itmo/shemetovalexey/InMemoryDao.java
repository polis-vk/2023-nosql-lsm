package ru.vk.itmo.shemetovalexey;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private SSTable ssTable;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable =
            new ConcurrentSkipListMap<>(comparator);

    private static final Comparator<MemorySegment> comparator = InMemoryDao::comparator;

    public InMemoryDao() {
    }

    public InMemoryDao(Config config) {
        ssTable = new SSTable(config);
    }

    private static byte getByte(MemorySegment memorySegment, long offset) {
        return memorySegment.get(ValueLayout.OfByte.JAVA_BYTE, offset);
    }

    static int comparator(MemorySegment left, MemorySegment right) {
        long offset = left.mismatch(right);
        if (offset == -1) {
            return Long.compare(left.byteSize(), right.byteSize());
        } else if (offset == left.byteSize() || offset == right.byteSize()) {
            return offset == left.byteSize() ? -1 : 1;
        } else {
            return Byte.compare(getByte(left, offset), getByte(right, offset));
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null && to == null) {
            subMap = memoryTable;
        } else if (from == null) {
            subMap = memoryTable.headMap(to);
        } else if (to == null) {
            subMap = memoryTable.tailMap(from);
        } else {
            subMap = memoryTable.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null) {
            return;
        }
        memoryTable.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (memoryTable.containsKey(key)) {
            return memoryTable.get(key);
        }
        return ssTable.get(key);
    }

    @Override
    public void close() throws IOException {
        ssTable.writeAndClose(memoryTable);
    }
}
