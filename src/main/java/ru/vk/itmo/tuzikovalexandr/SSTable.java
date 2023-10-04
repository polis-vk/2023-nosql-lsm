package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.*;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Set;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class SSTable {

    private static final Set<OpenOption> openOptions = Set.of(CREATE, READ, WRITE);
    private final Path filePath;
    private static final String FILE_PATH = "data";

    public SSTable(Config config) throws IOException {
        this.filePath = config.basePath().resolve(FILE_PATH);
    }

    public void saveMemData(Collection<Entry<MemorySegment>> entries) throws IOException {
        long offset = 0L;
        long memorySize = entries.stream().mapToLong(
                entry -> entry.key().byteSize() + entry.value().byteSize()
        ).sum() + Long.BYTES * entries.size() * 2L;

        try (FileChannel fc = FileChannel.open(filePath, openOptions)) {

            MemorySegment writeSegment = fc.map(READ_WRITE, 0, memorySize, Arena.global());

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
        try (FileChannel fc = FileChannel.open(filePath, READ)) {
            long offset = 0L;

            MemorySegment readSegment = fc.map(READ_ONLY, 0, Files.size(filePath), Arena.global());

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

        } catch (IOException e) {
            return null;
        }
    }
}
