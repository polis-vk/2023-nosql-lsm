package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;

public class SimpleSSTable {
    private long size;

    private final MemorySegment segment;

    public int id;

    private static class SSTableCreationException extends RuntimeException {
        public SSTableCreationException(Throwable cause) {
            super(cause);
        }
    }

    private static class SSTableRWException extends RuntimeException {
        public SSTableRWException(Throwable cause) {
            super(cause);
        }
    }

    SimpleSSTable(Path path, Arena arena) {
        this.id = Integer.parseInt(path.getFileName().toString().substring(8));
        try {
            if (Files.exists(path)) {
                this.size = Files.size(path);
            }
        } catch (IOException e) {
            throw new SSTableCreationException(e);
        }
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
            this.segment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }
    }

    public static Path writeSSTable(Collection<Entry<MemorySegment>> entries, Path path, int id) {
        Arena arena = Arena.ofShared();
        Path sstPath = Path.of(path.toAbsolutePath() + "/sstable_" + id);
        long dataSize = 0;
        for (var entry : entries) {
            dataSize += entry.value().byteSize() + entry.key().byteSize() + Long.BYTES*2;
        }
        try (var fileChannel = FileChannel.open(
                sstPath,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            var segment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataSize, arena);
            writeEntries(entries, segment);
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }
        arena.close();
        return sstPath;
    }

    private static void writeEntries(Collection<Entry<MemorySegment>> entries, MemorySegment segment) {
        long offset = 0;
        for (var entry : entries) {

            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
            offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

            MemorySegment.copy(entry.key(), 0, segment, offset, entry.key().byteSize());
            offset += entry.key().byteSize();

            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
            offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

            MemorySegment.copy(entry.value(), 0, segment, offset, entry.value().byteSize());
            offset += entry.value().byteSize();
        }
    }

    public MemorySegment get(MemorySegment key) {
        long offset = 0;
        while (offset < this.size) {
            var keySize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

            if (-1 == MemorySegment.mismatch(key, 0, key.byteSize(), segment, offset, offset + keySize)) {
                offset += keySize;
                var valSize = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                return segment.asSlice(offset, valSize);
            }
            offset += keySize;
            offset += segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
        }
        return null;
    }
}
