package ru.vk.itmo.reshetnikovaleksei;

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

public class SSTable {
    private static final String SS_TABLE_NAME = "ss_table.db";

    private final Path ssTablePath;
    private final Arena readDataArena;
    private final MemorySegment readDataSegment;

    public SSTable(Config config) throws IOException {
        this.ssTablePath = config.basePath().resolve(SS_TABLE_NAME);

        if (Files.exists(ssTablePath)) {
            try (FileChannel ssTableChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
                this.readDataArena = Arena.ofConfined();
                this.readDataSegment = ssTableChannel.map(
                        FileChannel.MapMode.READ_ONLY, 0, ssTableChannel.size(), readDataArena
                );
            }
        } else {
            this.readDataArena = null;
            this.readDataSegment = null;
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) throws IOException {
        if (this.readDataSegment == null) {
            return null;
        }

        long offset = 0;
        while (readDataSegment.byteSize() > offset) {
            long keySize = readDataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long valueSize = readDataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long mismatch = MemorySegment.mismatch(
                    readDataSegment, offset, offset + key.byteSize(),
                    key, 0, key.byteSize()
            );

            if (mismatch == -1) {
                return new BaseEntry<>(key, readDataSegment.asSlice(offset + keySize, valueSize));
            }

            offset += keySize + valueSize;
        }

        return null;
    }

    public void save(Collection<Entry<MemorySegment>> entries) throws IOException {
        try (FileChannel ssTableChannel = FileChannel.open(
                ssTablePath,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
             Arena writeDataArena = Arena.ofConfined()
        ) {
            long size = 0;
            for (Entry<MemorySegment> entry : entries) {
                size += entry.key().byteSize() + entry.value().byteSize() + 2 * Long.BYTES;
            }

            long offset = 0;
            MemorySegment ssTableMemorySegment = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, offset, size, writeDataArena);
            for (Entry<MemorySegment> entry : entries) {
                offset += setMemorySegmentSizeAndGetOffset(offset, entry.key(), ssTableMemorySegment);
                offset += setMemorySegmentSizeAndGetOffset(offset, entry.value(), ssTableMemorySegment);

                offset += copyToMemorySegmentAndGetOffset(offset, entry.key(), ssTableMemorySegment);
                offset += copyToMemorySegmentAndGetOffset(offset, entry.value(), ssTableMemorySegment);
            }
        }
    }

    public void close() {
        if (readDataArena != null) {
            readDataArena.close();
        }
    }

    private long setMemorySegmentSizeAndGetOffset(long offset,
                                                  MemorySegment entryMemorySegment,
                                                  MemorySegment ssTableMemorySegment) {
        ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entryMemorySegment.byteSize());
        return Long.BYTES;
    }

    private long copyToMemorySegmentAndGetOffset(long offset,
                                                 MemorySegment entryMemorySegment,
                                                 MemorySegment ssTableMemorySegment) {
        MemorySegment.copy(entryMemorySegment, 0, ssTableMemorySegment, offset, entryMemorySegment.byteSize());
        return entryMemorySegment.byteSize();
    }
}
