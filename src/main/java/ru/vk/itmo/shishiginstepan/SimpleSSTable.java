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
import java.util.Iterator;

public class SimpleSSTable {
    private final Path sstPath;
    private long size;

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

    SimpleSSTable(Path basePath) {
        sstPath = Path.of(basePath.toAbsolutePath() + "/sstable");

        try {
            if (!Files.exists(basePath)) {
                Files.createDirectory(basePath);
            }
            if (Files.exists(sstPath)) {
                this.size = Files.size(sstPath);
            } else {
                Files.createFile(sstPath);
            }
        } catch (IOException e) {
            throw new SSTableCreationException(e);
        }
    }

    public void writeEntries(Iterator<Entry<MemorySegment>> entries, long dataSize) {
        this.size = dataSize;
        try (var fileChannel = FileChannel.open(
                sstPath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            var file = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataSize, Arena.ofConfined());
            long offset = 0;
            while (entries.hasNext()) {
                var entry = entries.next();

                file.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

                MemorySegment.copy(entry.key(), 0, file, offset, entry.key().byteSize());
                offset += entry.key().byteSize();

                file.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

                MemorySegment.copy(entry.value(), 0, file, offset, entry.value().byteSize());
                offset += entry.value().byteSize();
            }
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }
    }

    public MemorySegment get(MemorySegment key) {
        try (var fileChannel = FileChannel.open(sstPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            var file = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, this.size, Arena.ofConfined());
            long offset = 0;
            while (offset < this.size) {
                var keySize = file.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

                if (-1 == MemorySegment.mismatch(key, 0, key.byteSize(), file, offset, offset + keySize)) {
                    offset += keySize;
                    var valSize = file.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                    offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                    return file.asSlice(offset, valSize);
                }
                offset += keySize;
                offset += file.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
            }
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }
        return null;
    }
}
