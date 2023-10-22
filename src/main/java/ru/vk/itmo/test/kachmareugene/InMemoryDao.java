package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.Long.min;
public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> memorySegmentComparatorImpl = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp =
            new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);

    private final Path ssTablesDir;
    private static final String SS_TABLE_NAME = "ssTable.txt";
    private final  Arena arena = Arena.ofShared();

    public InMemoryDao() {
        this.ssTablesDir = null;
    }

    public InMemoryDao(Path path) {
        this.ssTablesDir = path;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        SortedMap<MemorySegment, Entry<MemorySegment>> dataSlice;

        if (from == null && to == null) {
            dataSlice = mp;
        } else if (from == null) {
            dataSlice = mp.headMap(to);
        } else if (to == null) {
            dataSlice = mp.tailMap(from);
        } else {
            dataSlice = mp.subMap(from, to);
        }

        return dataSlice.values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (mp.containsKey(key)) {
            return mp.get(key);
        }

        if (ssTablesDir == null) {
            return null;
        }

        return SSTablesController.getEntryFromSSTable(ssTablesDir, SS_TABLE_NAME, key, memorySegmentComparatorImpl, arena);
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mp.put(entry.key(), entry);
    }

    private static class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(MemorySegment segment1, MemorySegment segment2) {

            long firstDiffByte = segment1.mismatch(segment2);

            if (firstDiffByte == -1) {
                return 0;
            }

            if (firstDiffByte == min(segment1.byteSize(), segment2.byteSize())) {
                return firstDiffByte == segment1.byteSize() ? -1 : 1;
            }

            return Byte.compare(segment1.get(ValueLayout.JAVA_BYTE, firstDiffByte),
                    segment2.get(ValueLayout.JAVA_BYTE, firstDiffByte));
        }
    }

    @Override
    public void close() throws IOException {
        SSTablesController.dump(ssTablesDir, SS_TABLE_NAME, mp, arena);
    }
}
