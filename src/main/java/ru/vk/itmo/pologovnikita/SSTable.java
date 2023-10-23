package ru.vk.itmo.pologovnikita;

import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentNavigableMap;

public class SSTable implements Closeable {
    private static final ValueLayout.OfLong LAYOUT = ValueLayout.JAVA_LONG_UNALIGNED;
    private static final Long LAYOUT_SIZE = LAYOUT.byteSize();
    private static final String FILE_NAME = "table.txt";
    private final Path path;
    //@Nullable
    private MemorySegment readPage;
    //@Nullable
    private Long fileSize;
    private Arena readArena;

    public SSTable(Path basePath) {
        path = basePath.resolve(FILE_NAME);
        initReadPage();
    }

    private void initReadPage() {
        if (!Files.exists(path)) {
            readPage = null;
        }
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            readArena = Arena.ofConfined();
            fileSize = Files.size(path);
            readPage = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, readArena);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void save(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        long size = getFileSize(map);
        try (var channel = FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);
             var arena = Arena.ofConfined()) {
            MemorySegment fileMemorySegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
            var writer = new MemorySegmentWriter(fileMemorySegment);
            for (var entry : map.values()) {
                writer.writeEntry(entry);
            }
        }
    }

    public MemorySegment get(MemorySegment key) {
        if (readPage == null) {
            return null;
        }
        return new MemorySegmentReader(readPage, fileSize).readFirstValueOfEntry(key);
    }

    private static long getFileSize(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        long size = 0L;
        for (var entry : map.values()) {
            size += getMemorySegmentEntrySize(entry);
        }
        return size;
    }

    private static long getMemorySegmentEntrySize(Entry<MemorySegment> entry) {
        long entrySize = getEntrySize(entry);
        return 2 * LAYOUT_SIZE + entrySize;
    }

    private static long getEntrySize(Entry<MemorySegment> entry) {
        return entry.key().byteSize() + entry.value().byteSize();
    }

    @Override
    public void close() throws IOException {
        readArena.close();
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
        private final Long fileSize;
        private Long offset = 0L;

        public MemorySegmentReader(MemorySegment fileMemorySegment, Long fileSize) {
            this.fileMemorySegment = fileMemorySegment;
            this.fileSize = fileSize;
        }

        public MemorySegment readFirstValueOfEntry(MemorySegment key) {
            MemorySegment result = null;
            while (offset < fileSize) {
                MemorySegment currentKey = read();
                if (key.mismatch(currentKey) == -1) {
                    result = read();
                }
            }
            return result;
        }

        private MemorySegment read() {
            long size = fileMemorySegment.get(LAYOUT, offset);
            offset += LAYOUT_SIZE;
            MemorySegment result = fileMemorySegment.asSlice(offset, size);
            offset += size;
            return result;
        }
    }
}
