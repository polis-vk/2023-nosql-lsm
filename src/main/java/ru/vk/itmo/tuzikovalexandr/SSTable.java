package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Set;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class SSTable {

    private static final Set<OpenOption> openOptions = Set.of(
            StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
    );
    private final Path filePath;
    private final Arena readArena;
    private final MemorySegment readSegment;
    private static final String FILE_PATH = "data";

    public SSTable(Config config) throws IOException {
        this.filePath = config.basePath().resolve(FILE_PATH);

        readArena = Arena.ofConfined();

        if (Files.notExists(filePath)) {
            readSegment = null;
            return;
        }

        try (FileChannel fc = FileChannel.open(filePath, StandardOpenOption.READ)) {
            readSegment = fc.map(READ_ONLY, 0, Files.size(filePath), readArena);
        }
    }

    public void saveMemData(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (!readArena.scope().isAlive()) {
            return;
        }

        readArena.close();

        long offset = 0L;
        long memorySize = entries.stream().mapToLong(
                entry -> entry.key().byteSize() + entry.value().byteSize()
        ).sum() + Long.BYTES * entries.size() * 2L;

        try (FileChannel fc = FileChannel.open(filePath, openOptions)) {

            MemorySegment writeSegment = fc.map(READ_WRITE, 0, memorySize, Arena.ofConfined());

            for (Entry<MemorySegment> entry : entries) {
                writeSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
                offset += Long.BYTES;
                writeSegment.asSlice(offset).copyFrom(entry.key());
                offset += entry.key().byteSize();

                writeSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                writeSegment.asSlice(offset).copyFrom(entry.value());
                offset += entry.value().byteSize();
            }
        }
    }

    public Entry<MemorySegment> readData(MemorySegment key) {
        if (readSegment == null) {
            return null;
        }

        long offset = 0L;

        while (offset < readSegment.byteSize()) {
            long keySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            long valueSize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + keySize);

            if (keySize != key.byteSize()) {
                offset += keySize + valueSize + Long.BYTES;
                continue;
            }

            MemorySegment keySegment = readSegment.asSlice(offset, keySize);
            offset += keySize + Long.BYTES;

            if (key.mismatch(keySegment) == -1) {
                MemorySegment valueSegment = readSegment.asSlice(offset, valueSize);
                return new BaseEntry<>(keySegment, valueSegment);
            }

            offset += valueSize;
        }

        return null;
    }
}
