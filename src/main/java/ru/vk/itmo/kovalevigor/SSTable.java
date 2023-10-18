package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class SSTable {

    private final Path path;
    public static final Comparator<MemorySegment> COMPARATOR = UtilsMemorySegment::compare;

    public SSTable(final Path path) {
        this.path = path;
    }

    public SortedMap<MemorySegment, Entry<MemorySegment>> load(final Arena arena, final long limit) throws IOException {
        final SortedMap<MemorySegment, Entry<MemorySegment>> map = new TreeMap<>(COMPARATOR);
        if (Files.notExists(path)) {
            return map;
        }

        try (FileChannel readerChannel = FileChannel.open(
                path,
                StandardOpenOption.READ)
        ) {
            final MemorySegment memorySegment = readerChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    readerChannel.size(),
                    arena
            );
            final MemoryEntryReader reader = new MemoryEntryReader(memorySegment);

            Entry<MemorySegment> entry = reader.readEntry(limit);
            while (entry != null) {
                map.put(entry.key(), entry);
                entry = reader.readEntry(limit);
            }
            return map;
        }
    }

    public SortedMap<MemorySegment, Entry<MemorySegment>> load(final Arena arena) throws IOException {
        return load(arena, 0);
    }

    public Entry<MemorySegment> get(final MemorySegment key, final Arena segmentArena) throws IOException {
        if (Files.notExists(path)) {
            return null;
        }

        try (
                Arena readerArena = Arena.ofConfined();
                FileChannel readerChannel = FileChannel.open(path, StandardOpenOption.READ)
        ) {

            final MemorySegment memorySegment = readerChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    readerChannel.size(),
                    readerArena
            );
            final MemoryEntryReader reader = new MemoryEntryReader(memorySegment);

            long prevOffset = reader.getOffset();
            Entry<MemorySegment> entry = reader.readEntry();
            while (entry != null) {
                if (UtilsMemorySegment.findDiff(key, entry.key()) == -1) {
                    return MemoryEntryReader.mapEntry(
                            readerChannel,
                            prevOffset,
                            entry.key().byteSize(),
                            entry.value().byteSize(),
                            segmentArena
                    );
                }
                prevOffset = reader.getOffset();
                entry = reader.readEntry();
            }
        }
        return null;
    }

    public void write(final SortedMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        try (Arena arena = Arena.ofConfined(); FileChannel writerChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE)
        ) {
            final MemorySegment memorySegment = writerChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    MemoryEntryWriter.getTotalMapSize(map),
                    arena
            );
            final MemoryEntryWriter writer = new MemoryEntryWriter(memorySegment);

            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : map.entrySet()) {
                writer.putEntry(entry);
            }
        }
    }
}
