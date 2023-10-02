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

    SimpleSSTable(Path basePath) {
        sstPath = Path.of(basePath.toAbsolutePath() + "/sstable");
        try {
            if (!Files.exists(basePath)) {
                Files.createDirectory(basePath);
            }
            if (!Files.exists(sstPath)) {
                Files.createFile(sstPath);
            } else {
                this.size = Files.size(sstPath);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeEntries(Iterator<Entry<MemorySegment>> entries, long dataSize) {
        this.size = dataSize;
        try (var fileChannel = FileChannel.open(sstPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            var file = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataSize, Arena.ofConfined());
            long j = 0;
            while (entries.hasNext()) {
                var entry = entries.next();
                file.set(ValueLayout.JAVA_LONG_UNALIGNED, j, entry.key().byteSize());
                //MemorySegment.copy();//   TODO переписать на копировании мем.сегментов
                j += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                for (int i = 0; i < entry.key().byteSize(); i++) {
                    file.set(ValueLayout.JAVA_BYTE, j, entry.key().getAtIndex(ValueLayout.JAVA_BYTE, i));
                    j++;
                }
                file.set(ValueLayout.JAVA_LONG_UNALIGNED, j, entry.value().byteSize());
                j += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                for (int i = 0; i < entry.value().byteSize(); i++) {
                    file.set(ValueLayout.JAVA_BYTE, j, entry.value().getAtIndex(ValueLayout.JAVA_BYTE, i));
                    j++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
        }
        return null;
    }
}