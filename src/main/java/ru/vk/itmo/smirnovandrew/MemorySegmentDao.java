package ru.vk.itmo.smirnovandrew;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.test.smirnovandrew.MyFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Path tablePath;

    private static final String table = "table.txt";

    private final MemorySegment ssTable;

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> segments =
            new ConcurrentSkipListMap<>(segmentComparator);

    private static final Comparator<MemorySegment> segmentComparator = (o1, o2) -> {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }
        try {
            return Byte.compare(o1.getAtIndex(ValueLayout.JAVA_BYTE, mismatch),
                    o2.getAtIndex(ValueLayout.JAVA_BYTE, mismatch));
        } catch (IndexOutOfBoundsException e) {
            return 0;
        }
    };

    public MemorySegmentDao(Config config) {
        MemorySegment ssTable1;

        tablePath = config.basePath().resolve(table);
        try {
            long fileSize = 0;
            try {
                fileSize = Files.size(tablePath);
            } catch (IOException e) {
                Files.createFile(tablePath);
                System.err.println("Can't find table file " + e);
            }

            ssTable1 = assertFile(tablePath, fileSize,
                    FileChannel.MapMode.READ_ONLY, StandardOpenOption.READ);
        } catch (IOException e) {
            System.err.println("Table path is incorrect");
            ssTable1 = null;
        }
        ssTable = ssTable1;
    }

    public MemorySegmentDao() {
        tablePath = null;
        ssTable = null;
    }

    private static MemorySegment assertFile(Path path, long fileSize,
                                            FileChannel.MapMode mapMode, OpenOption... openOptions)
            throws IOException {
        try (final var fc = FileChannel.open(path, openOptions)) {
            return fc.map(mapMode, 0, fileSize, Arena.ofAuto());
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {

        if (from == null && to == null) {
            return segments.values().iterator();
        }
        if (from == null) {
            return segments.headMap(to, false).values().iterator();
        }
        if (to == null) {
            return segments.tailMap(from, true).values().iterator();
        }

        return segments.subMap(from, true, to, false).values().iterator();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var entry = segments.get(key);
        if (entry != null) {
            return entry;
        }

        if (ssTable == null) {
            return null;
        }

        long fileSize = ssTable.byteSize();

        long offset = 0;
        while (offset < fileSize) {
            long keyLen = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final var currentKey = ssTable.asSlice(offset, keyLen);
            offset += keyLen + Long.BYTES;

            long valueLen = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            final var currentValue = valueLen != -1 ? ssTable.asSlice(offset, valueLen) : null;
            offset += valueLen + Long.BYTES;

            if (keyLen != key.byteSize()) {
                continue;
            }

            if (segmentComparator.compare(currentKey, key) == 0) {
                return new BaseEntry<>(currentKey, currentValue);
            }
        }


        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        segments.put(entry.key(), entry);
    }

    private long calculateSize() {
        long res = 0;
        for (final var entry : segments.values()) {
            long valueLen = entry.value() == null ? 0 : entry.value().byteSize();
            res += 2 * Long.BYTES + entry.key().byteSize() + valueLen;
        }
        return res;
    }

    private long writeSegment(MemorySegment segment, MemorySegment table, long offset) {
        if (segment == null) {
            table.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);

            return offset + Long.BYTES;
        }

        table.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, segment.byteSize());
        MemorySegment.copy(segment, 0, table, offset + Long.BYTES, segment.byteSize());

        return offset + Long.BYTES + segment.byteSize();
    }

    @Override
    public void close() throws IOException {
        long fileSize = calculateSize();

        final var ssTableClose = assertFile(tablePath, fileSize, FileChannel.MapMode.READ_WRITE,
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE,
                StandardOpenOption.READ, StandardOpenOption.CREATE);

        System.out.println(ssTableClose);

        long offset = 0;
        for (final var entry : segments.values()) {
            offset = writeSegment(entry.key(), ssTableClose, offset);
            offset = writeSegment(entry.value(), ssTableClose, offset);
        }
    }

}
