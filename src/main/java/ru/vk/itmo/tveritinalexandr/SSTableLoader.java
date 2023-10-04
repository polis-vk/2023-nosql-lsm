package ru.vk.itmo.tveritinalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;

public class SSTableLoader {
    private static final MemorySegmentComparator comparator = MemorySegmentComparator.INSTANCE;
    private final Path ssTableFilePath;
    private long offset;

    public SSTableLoader(Path ssTableFilePath) {
        this.ssTableFilePath = ssTableFilePath;
        }

    public SortedMap<MemorySegment, Entry<MemorySegment>> load() {
        offset = 0;

        SortedMap<MemorySegment, Entry<MemorySegment>> map = new ConcurrentSkipListMap<>(comparator);

        if (ssTableFilePath == null || !Files.exists(ssTableFilePath)) return null;
        long fileSize;
        try {
            fileSize = Files.size(ssTableFilePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        MemorySegment lastMemorySegment = null;
        Arena arena = Arena.ofConfined();
        MemorySegment keySegment = null;

        try (FileChannel channel = FileChannel.open(ssTableFilePath, StandardOpenOption.READ)) {
            MemorySegment fileSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            while (offset < fileSize) {
                keySegment = getMemorySegment(fileSegment);
                lastMemorySegment = getMemorySegment(fileSegment);
                map.put(keySegment, new BaseEntry<>(keySegment, lastMemorySegment));
            }

            return map;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Entry<MemorySegment> findInSSTable(MemorySegment key) {
        offset = 0;
        if (ssTableFilePath == null || !Files.exists(ssTableFilePath)) return null;
        long fileSize;
        try {
            fileSize = Files.size(ssTableFilePath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        MemorySegment lastMemorySegment = null;
        Arena arena = Arena.ofConfined();
        MemorySegment keySegment = null;
        try (FileChannel channel = FileChannel.open(ssTableFilePath, StandardOpenOption.READ)) {
            MemorySegment fileSegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            while (offset < fileSize) {
                keySegment = getMemorySegment(fileSegment);
                if (comparator.compare(key, keySegment) == 0) {
                    lastMemorySegment = getMemorySegment(fileSegment);
                    break;
                }
            }

            return lastMemorySegment == null ? null : new BaseEntry<>(keySegment, lastMemorySegment);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MemorySegment getMemorySegment(MemorySegment memorySegment) throws IOException {
        long size = memorySegment.get(JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES + size;
        return memorySegment.asSlice(offset - size, size);
    }
}
