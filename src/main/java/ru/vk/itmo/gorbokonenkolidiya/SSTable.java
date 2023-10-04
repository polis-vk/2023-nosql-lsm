package ru.vk.itmo.gorbokonenkolidiya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
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

import static ru.vk.itmo.gorbokonenkolidiya.AbstractDao.compare;

public class SSTable {
    private static final String FILE_NAME = "data.db";
    private final Path tablePath;

    public SSTable(Config config) {
        tablePath = config.basePath().resolve(FILE_NAME);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (Files.notExists(tablePath)) {
            return null;
        }

        try (FileChannel fileChannel = FileChannel.open(tablePath, StandardOpenOption.READ)) {
            MemorySegment data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), Arena.global());

            long offset = 0;
            while (offset < data.byteSize()) {
                long keySize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                long valueSize = data.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                if (keySize == key.byteSize()) {
                    MemorySegment foundKey = data.asSlice(offset, keySize);
                    offset += keySize;

                    if (compare(foundKey, key) == 0) {
                        MemorySegment foundValue = data.asSlice(offset, valueSize);

                        return new BaseEntry<>(foundKey, foundValue);
                    }
                } else {
                    offset += keySize;
                }

                offset += valueSize;
            }

            return null;
        } catch (IOException e) {
            return null;
        }
    }

    // SSTable structure: [keySize][valueSize][key][value]
    public void flush(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (entries.isEmpty()) return;

        long size = 0;
        for (Entry<MemorySegment> entry : entries) {
            size += entry.key().byteSize() + entry.value().byteSize() + 2L * Long.BYTES;
        }

        try (FileChannel ssTable = FileChannel.open(tablePath, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            MemorySegment memorySegment = ssTable.map(FileChannel.MapMode.READ_WRITE, 0, size, Arena.global());

            long offset = 0;
            for (Entry<MemorySegment> entry : entries) {
                // Write keySize
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;

                // Write valueSize
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;

                // Write key
                memorySegment.asSlice(offset).copyFrom(entry.key());
                offset += entry.key().byteSize();

                // Write value
                memorySegment.asSlice(offset).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        }
    }
}
