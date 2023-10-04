package ru.vk.itmo.pologovnikita;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.concurrent.ConcurrentNavigableMap;

public class SSTable {
    private static final Comparator<MemorySegment> memorySegmentComparator = new MemorySegmentComparator();
    private static final ValueLayout.OfLong LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final Long LAYOUT_SIZE = LAYOUT.byteSize();
    private static final String FILE_NAME = "table.txt";
    private final Arena arena = Arena.ofConfined();
    private final Path path;

    public SSTable(Path basePath) {
        path = basePath.resolve(FILE_NAME);
    }

    public void save(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        long size = getFileSize(map);
        try (var channel = FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {
            var fileMemorySegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
            var writer = new MemorySegmentWriter(fileMemorySegment);
            for (var entry : map.values()) {
                writer.writeEntry(entry);
            }
        }
    }

    public MemorySegment get(MemorySegment key) {
        if (!Files.exists(path)) {
            return null;
        }
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            var fileSize = Files.size(path);
            var fileMemorySegment = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            return new MemorySegmentReader(fileMemorySegment).findFirstValue(key, fileSize);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static long getFileSize(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        var size = 0L;
        for (var entry : map.values()) {
            size = getEntrySize(size, entry);
        }
        return size;
    }

    private static long getEntrySize(long size, Entry<MemorySegment> entry) {
        var entrySize = entry.key().byteSize() + entry.value().byteSize();
        size += 2 * LAYOUT_SIZE + entrySize;
        return size;
    }

    static class MemorySegmentWriter {
        private final MemorySegment fileMemorySegment;
        private Long offset = 0L;

        public MemorySegmentWriter(MemorySegment fileMemorySegment) {
            this.fileMemorySegment = fileMemorySegment;
        }

        public void writeEntry(Entry<MemorySegment> entry) {
            writeMemorySegment(entry.key());
            writeMemorySegment(entry.value());
        }

        private void writeMemorySegment(MemorySegment segment) {
            fileMemorySegment.set(LAYOUT, offset, segment.byteSize());
            MemorySegment.copy(segment, 0, fileMemorySegment, offset + LAYOUT_SIZE, segment.byteSize());
            offset += LAYOUT_SIZE + segment.byteSize();
        }
    }

    static class MemorySegmentReader {
        private final MemorySegment fileMemorySegment;
        private Long offset = 0L;

        public MemorySegmentReader(MemorySegment fileMemorySegment) {
            this.fileMemorySegment = fileMemorySegment;
        }

        public MemorySegment findFirstValue(MemorySegment key, Long fileSize) {
            MemorySegment result = null;
            while (offset < fileSize) {
                var currentKey = read();
                if (memorySegmentComparator.compare(key, currentKey) == 0) {
                    result = read();
                }
            }
            return result;
        }

        private MemorySegment read() {
            var size = fileMemorySegment.get(LAYOUT, offset);
            offset += LAYOUT_SIZE;
            var result = fileMemorySegment.asSlice(offset, size);
            offset += size;
            return result;
        }
    }
}
