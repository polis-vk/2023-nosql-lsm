package ru.vk.itmo.tyapuevdmitrij;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

public final class NmapBuffer {

    private NmapBuffer() {
        throw new IllegalStateException("Utility class");
    }

    static MemorySegment getReadBufferFromSsTable(Path ssTablePath, Arena readArena) {
        MemorySegment buffer;
        boolean created = false;
        try (FileChannel channel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(ssTablePath), readArena);
            created = true;
        } catch (IOException e) {
            buffer = null;
        } finally {
            if (!created) {
                readArena.close();
            }
        }
        return buffer;
    }

    static MemorySegment getWriteBufferToSsTable(Long writeBytes,
                                                 Path ssTablePath,
                                                 int ssTablesQuantity,
                                                 Arena writeArena,
                                                 boolean compactionFlag) throws IOException {
        MemorySegment buffer;
        Path path;
        if (compactionFlag) {
            path = ssTablePath.resolve(StorageHelper.COMPACTED_FILE_NAME);
        } else {
            path = ssTablePath.resolve(StorageHelper.SS_TABLE_FILE_NAME + ssTablesQuantity);
        }
        try (FileChannel channel = FileChannel.open(path, EnumSet.of(StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING))) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0,
                    writeBytes, writeArena);
        }
        return buffer;
    }
}


