package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.Long.min;
import static java.nio.file.StandardOpenOption.*;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {


    private final Comparator<MemorySegment> memorySegmentComparatorImpl = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp = new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);

    private final Path ssTablesDir;
    private final String ssTableName = "ssTable.txt";

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

    private MemorySegment searchKeyInFile(MemorySegment mapped, MemorySegment key, long maxSize) {
        long offset = 0L;
        long kSize, vSize;

        while (offset < maxSize) {
            offset = alignmentBy(offset, 8);
            kSize = mapped.get(ValueLayout.JAVA_LONG, offset);
            offset += Long.BYTES;
            vSize = mapped.get(ValueLayout.JAVA_LONG, offset);
            offset += Long.BYTES;

            if (memorySegmentComparatorImpl.compare(key, mapped.asSlice(offset, kSize)) == 0) {
                return mapped.asSlice(offset + kSize, vSize);
            }
            offset += kSize + vSize;
        }
        return null;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (mp.containsKey(key)) {
            return mp.get(key);
        }

        // fixme check existence
        if (ssTablesDir == null) {
            return null;
        }
        Set<OpenOption> options = Set.of(READ);
        try (FileChannel channel = FileChannel.open(ssTablesDir.resolve(ssTableName), options)) {
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofAuto());
            return new BaseEntry<>(key, searchKeyInFile(mapped, key, channel.size()));
        } catch (IOException e) {
            return null;
        }
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


    private long dumpSegmentSize(MemorySegment mapped, long size, long offset) {
        long formattedOffset = alignmentBy(offset, 8);
        mapped.set(ValueLayout.JAVA_LONG, formattedOffset, size);
        return formattedOffset + Long.BYTES;
    }
    private long dumpSegment(MemorySegment mapped, MemorySegment data, long offset) {
        MemorySegment.copy(data, 0, mapped, offset, data.byteSize());
        return offset + data.byteSize();
    }


    private long alignmentBy(long data, long by) {
        return data % by != 0 ? data + (by - data % by) : data;
    }
    @Override
    public void close() throws IOException {
        if (ssTablesDir == null) {
            return;
        }
        Set<OpenOption> options = Set.of(WRITE, READ, StandardOpenOption.CREATE);
        try (FileChannel channel = FileChannel.open(ssTablesDir.resolve(ssTableName), options)) {
            long currOffset = 0L;
            long allDataLen = 0L;
            for (var kv : mp.values()) {
                allDataLen += alignmentBy(kv.key().byteSize() + kv.value().byteSize() + 16, 8);
            }
            MemorySegment mappedSegment = channel.map(
                    FileChannel.MapMode.READ_WRITE, currOffset, allDataLen, Arena.ofAuto());

            for (var kv : mp.values()) {
                currOffset = dumpSegmentSize(mappedSegment, kv.key().byteSize(), currOffset);
                currOffset = dumpSegmentSize(mappedSegment, kv.value().byteSize(), currOffset);
                currOffset = dumpSegment(mappedSegment, kv.key(), currOffset);
                currOffset = dumpSegment(mappedSegment, kv.value(), currOffset);
            }

        } finally {
            mp.clear();
        }
    }
}
