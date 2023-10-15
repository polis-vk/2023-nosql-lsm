package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class Storage implements Closeable {

    private static final String DB_PREFIX = "data";
    private static final String DB_EXTENSION = ".db";
    private static final long FILE_PREFIX = Long.BYTES;
    private final Arena arena;
    private final List<MemorySegment> ssTables;

    public Storage(Arena arena, List<MemorySegment> ssTables) {
        this.arena = arena;
        this.ssTables = ssTables;
    }

    public static Storage load(Config config) {
        Path path = config.basePath();

        List<MemorySegment> ssTables = new ArrayList<>();
        Arena arena = Arena.ofShared();

        try (var files = Files.list(path)) {
            files
                    .filter(p -> p.getFileName().toString().startsWith(DB_PREFIX))
                    .filter(p -> p.getFileName().toString().endsWith(DB_EXTENSION))
                    .sorted()
                    .forEach(p -> {
                        try (FileChannel channel = FileChannel.open(p, StandardOpenOption.READ)) {
                            MemorySegment segment = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
                            ssTables.add(segment);
                        } catch (IOException e) {
                            throw new RuntimeException("Can't open file", e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException("Can't load storage", e);
        }

        Collections.reverse(ssTables);
        return new Storage(arena, ssTables);
    }

    public static void save(Config config, Collection<Entry<MemorySegment>> entries, Storage storage) {
        if (storage.arena.scope().isAlive()) {
            storage.arena.close();
        }

        if (entries.isEmpty()) {
            return;
        }

        int nextSSTable = storage.ssTables.size() + 1;
        Path path = config.basePath().resolve(DB_PREFIX + nextSSTable + DB_EXTENSION);

        long indicesSize = (long) Long.BYTES * (entries.size() + 1);
        long sizeOfNewSSTable = indicesSize;
        for (Entry<MemorySegment> entry : entries) {
            sizeOfNewSSTable += 2 * Long.BYTES + entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize());
        }


        try (Arena arenaSave = Arena.ofConfined();
             var channel = FileChannel.open(path,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {

            MemorySegment newSSTable = channel.map(FileChannel.MapMode.READ_WRITE, 0, sizeOfNewSSTable, arenaSave);

            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entries.size());

            long offsetIndex = Long.BYTES;
            long offsetData = indicesSize;
            for (Entry<MemorySegment> entry : entries) {
                newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, offsetData);
                offsetIndex += Long.BYTES;
                offsetData += saveEntrySegment(newSSTable, entry, offsetData);
            }

        } catch (IOException e) {
            throw new RuntimeException("Can't create new SSTable", e);
        }
    }

    public static long saveEntrySegment(MemorySegment newSSTable, Entry<MemorySegment> entry, long offsetData) {
        newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, entry.key().byteSize());
        offsetData += Long.BYTES;
        MemorySegment.copy(entry.key(), 0, newSSTable, offsetData, entry.key().byteSize());
        offsetData += entry.key().byteSize();
        if (entry.value() != null) {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, entry.value().byteSize());
            offsetData += Long.BYTES;
            MemorySegment.copy(entry.value(), 0, newSSTable, offsetData, entry.value().byteSize());
            offsetData += entry.value().byteSize();
        } else {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, -1);
            offsetData += Long.BYTES;
        }
        return offsetData;
    }

    private long binarySearchUpperBoundOrEquals(MemorySegment ssTable, MemorySegment key) {
        long left = 0;
        long right = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0) - 1;
        while (left <= right) {
            long mid = (left + right) / 2;

            long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * (mid + FILE_PREFIX));
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

            MemorySegment keySegment = ssTable.asSlice(offset + Long.BYTES, keySize);
            int cmp = MemorySegmentComparator.INSTANCE.compare(keySegment, key);
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private Entry<MemorySegment> getEntryByIndex(MemorySegment ssTable, long index) {
        long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * (index + FILE_PREFIX));
        long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        MemorySegment keySegment = ssTable.asSlice(offset + Long.BYTES, keySize);
        offset += Long.BYTES + keySize;

        long valueSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        if (valueSize == -1) {
            return new BaseEntry<>(keySegment, null);
        }

        MemorySegment valueSegment = ssTable.asSlice(offset, valueSize);
        return new BaseEntry<>(keySegment, valueSegment);
    }

    public Iterator<Entry<MemorySegment>> iterator(MemorySegment ssTable, MemorySegment from, MemorySegment to) {
        long left = binarySearchUpperBoundOrEquals(ssTable, from);
        long right = binarySearchUpperBoundOrEquals(ssTable, to);

        return new Iterator<>() {
            long current = left;

            @Override
            public boolean hasNext() {
                return current < right;
            }

            @Override
            public Entry<MemorySegment> next() {
                return getEntryByIndex(ssTable, current++);
            }
        };
    }

    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }
}
