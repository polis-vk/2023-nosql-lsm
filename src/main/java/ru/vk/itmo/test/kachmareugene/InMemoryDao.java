package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

import static java.lang.Long.min;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> memorySegmentComparatorImpl = new MemorySegmentComparator();
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mp =
            new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);
    private final Path ssTablesDir;
    private final List<FileChannel> ssTables = new ArrayList<>();
    private final List<FileChannel> ssTablesIndexes = new ArrayList<>();
    private static final String SS_TABLE_COMMON_PREF = "ssTable";

    // index format: (long) keyOffset, (long) keyLen, (long) valueOffset, (long) valueLen
    private static final String INDEX_COMMON_PREF = "index";
    private final Arena arena;

    private void openFiles(Path dir, String fileNamePref, List<FileChannel> storage) {
        try (Stream<Path> tabels = Files.find(dir, 1,
                (path, ignore) -> path.getFileName().startsWith(fileNamePref))) {
            tabels.forEach(t -> {
                try {
                    storage.add(FileChannel.open(t, READ));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot open %s %s", fileNamePref, e.getMessage()));
        }
    }

    public InMemoryDao(Config conf) {
        this.arena = Arena.ofShared();
        this.ssTablesDir = conf.basePath();

        openFiles(conf.basePath(), SS_TABLE_COMMON_PREF, ssTables);
        openFiles(conf.basePath(), INDEX_COMMON_PREF, ssTablesIndexes);
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

    // fixme give offset in file
    private MemorySegment searchKeyInFile(MemorySegment mapped, MemorySegment key, long maxSize) {
        long offset = 0L;
        long keyByteSize;
        long valueByteSize;

        while (offset < maxSize) {
            offset = alignmentBy(offset, Long.BYTES);
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

        try (FileChannel channel = FileChannel.open(ssTablesDir.resolve(SS_TABLE_COMMON_PREF), READ)) {
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

    private long dumpLong(MemorySegment mapped, long value, long offset) {
        mapped.set(ValueLayout.JAVA_LONG, offset, value);
        return offset + Long.BYTES;
    }

    private long dumpSegment(MemorySegment mapped, MemorySegment data, long offset) {
        MemorySegment.copy(data, 0, mapped, offset, data.byteSize());
        return offset + data.byteSize();
    }

    @Override
    public void close() throws IOException {
        if (ssTablesDir == null) {
            mp.clear();
            return;
        }

        Set<OpenOption> options = Set.of(WRITE, READ, CREATE);

        try (FileChannel ssTableChannel =
                     FileChannel.open(ssTablesDir.resolve(SS_TABLE_COMMON_PREF + ssTables.size()), options);
             FileChannel indexChannel =
                     FileChannel.open(ssTablesDir.resolve(INDEX_COMMON_PREF + ssTables.size()), options)) {

            long ssTableLenght = 0L;
            long indexLength = mp.size() * (long) Long.BYTES * 3;

            for (var kv : mp.values()) {
                ssTableLenght +=
                        kv.key().byteSize() + kv.value().byteSize();
            }

            long currOffsetSSTable = 0L;
            long currOffsetIndex = 0L;

            MemorySegment mappedSSTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetSSTable, ssTableLenght, Arena.ofConfined());

            MemorySegment mappedIndex = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetIndex, indexLength, Arena.ofConfined());

            for (var kv : mp.values()) {
                currOffsetIndex = dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = dumpSegment(mappedSSTable, kv.key(), currOffsetSSTable);
                currOffsetIndex = dumpLong(mappedIndex, kv.key().byteSize(), currOffsetIndex);

                currOffsetIndex = dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = dumpSegment(mappedSSTable, kv.value(), currOffsetSSTable);
                currOffsetIndex = dumpLong(mappedIndex, kv.value().byteSize(), currOffsetIndex);
            }
        } finally {
            mp.clear();
        }
    }
}
