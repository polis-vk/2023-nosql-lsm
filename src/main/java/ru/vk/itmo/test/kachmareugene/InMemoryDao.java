package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.Long.min;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.nio.file.StandardOpenOption.CREATE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> memorySegmentComparatorImpl = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp =
            new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);

    private final Path ssTablesDir;
    private static final String SS_TABLE_NAME = "ssTable.txt";

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
        long keyByteSize;
        long valueByteSize;

        while (offset < maxSize) {
            offset = alignmentBy(offset, 8);
            keyByteSize = mapped.get(ValueLayout.JAVA_LONG, offset);
            offset += Long.BYTES;
            valueByteSize = mapped.get(ValueLayout.JAVA_LONG, offset);
            offset += Long.BYTES;

            if (memorySegmentComparatorImpl.compare(key, mapped.asSlice(offset, keyByteSize)) == 0) {
                return mapped.asSlice(offset + keyByteSize, valueByteSize);
            }
            offset += keyByteSize + valueByteSize;
        }
        return null;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (mp.containsKey(key)) {
            return mp.get(key);
        }

        if (ssTablesDir == null) {
            return null;
        }

        try (FileChannel channel = FileChannel.open(ssTablesDir.resolve(SS_TABLE_NAME), READ)) {
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
        return data % by == 0L ? data : data + (by - data % by);
    }

    @Override
    public void close() throws IOException {
        if (ssTablesDir == null) {
            mp.clear();
            return;
        }

        Set<OpenOption> options = Set.of(WRITE, READ, CREATE);
        try (FileChannel channel = FileChannel.open(ssTablesDir.resolve(SS_TABLE_NAME), options)) {
            long dataLenght = 0L;

            for (var kv : mp.values()) {
                dataLenght += alignmentBy(kv.key().byteSize() + kv.value().byteSize() + 16, 8);
            }

            long currOffset = 0L;
            MemorySegment mappedSegment = channel.map(
                    FileChannel.MapMode.READ_WRITE, currOffset, dataLenght, Arena.ofAuto());

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
