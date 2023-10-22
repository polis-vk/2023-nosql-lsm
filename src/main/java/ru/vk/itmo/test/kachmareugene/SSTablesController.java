package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedMap;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public final class SSTablesController {
    private SSTablesController() {

    }

    private static long dumpSegmentSize(MemorySegment mapped, long size, long offset) {
        mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, size);
        return offset + Long.BYTES;
    }

    private static long dumpSegment(MemorySegment mapped, MemorySegment data, long offset) {
        MemorySegment.copy(data, 0, mapped, offset, data.byteSize());
        return offset + data.byteSize();
    }

    public static Entry<MemorySegment> getEntryFromSSTable(Path ssTablesDir, String ssTableName,
                                                           MemorySegment key, Comparator<MemorySegment> comp,
                                                           Arena arena) {
        try (FileChannel channel = FileChannel.open(ssTablesDir.resolve(ssTableName), READ)) {
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
            return new BaseEntry<>(key, SSTablesController.searchKeyInFile(mapped, key, channel.size(), comp));
        } catch (IOException e) {
            return null;
        }
    }

    public static MemorySegment searchKeyInFile(MemorySegment mapped, MemorySegment key,
                                                long maxSize, Comparator<MemorySegment> comp) {
        long offset = 0L;
        long keyByteSize;
        long valueByteSize;

        while (offset < maxSize) {
            keyByteSize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            valueByteSize = mapped.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            if (comp.compare(key, mapped.asSlice(offset, keyByteSize)) == 0) {
                return mapped.asSlice(offset + keyByteSize, valueByteSize);
            }
            offset += keyByteSize + valueByteSize;
        }
        return null;
    }

    public static void dump(Path ssTablesDir, String ssTableName,
                            SortedMap<MemorySegment, Entry<MemorySegment>> memTable,
                            Arena sharedArena) throws IOException {
        if (ssTablesDir == null) {
            memTable.clear();
            sharedArena.close();
            return;
        }

        Set<OpenOption> options = Set.of(WRITE, READ, CREATE);
        try (FileChannel channel = FileChannel.open(ssTablesDir.resolve(ssTableName), options);
             Arena arena = Arena.ofConfined()) {
            long dataLenght = 0L;

            for (var kv : memTable.values()) {
                dataLenght +=
                        kv.key().byteSize() + kv.value().byteSize() + 2 * Long.BYTES;
            }

            long currOffset = 0L;
            MemorySegment mappedSegment = channel.map(
                    FileChannel.MapMode.READ_WRITE, currOffset, dataLenght, arena);

            for (var kv : memTable.values()) {
                currOffset = dumpSegmentSize(mappedSegment, kv.key().byteSize(), currOffset);
                currOffset = dumpSegmentSize(mappedSegment, kv.value().byteSize(), currOffset);
                currOffset = dumpSegment(mappedSegment, kv.key(), currOffset);
                currOffset = dumpSegment(mappedSegment, kv.value(), currOffset);
            }
        } finally {
            memTable.clear();
            sharedArena.close();
        }
    }
}
