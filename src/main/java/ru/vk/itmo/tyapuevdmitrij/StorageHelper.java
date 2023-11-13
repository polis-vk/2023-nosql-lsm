package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class StorageHelper {
    protected static final String SS_TABLE_FILE_NAME = "ssTable";

    protected static final String COMPACTED_FILE_NAME = "compact";
    protected long memTableEntriesCount;

    static int findSsTablesQuantity(Path ssTablePath) {
        File dir = new File(ssTablePath.toUri());
        File[] files = dir.listFiles();
        if (files == null) {
            return 0;
        }
        long countSsTables = 0L;
        for (File file : files) {
            if (file.isFile() && file.getName().contains(SS_TABLE_FILE_NAME)) {
                countSsTables++;
            }
        }
        return (int) countSsTables;
    }

    static void deleteOldSsTables(Path ssTablePath) {
        File directory = new File(ssTablePath.toUri());
        if (!directory.exists() && !directory.isDirectory()) {
            return;
        }
        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (file.getName().contains(SS_TABLE_FILE_NAME)) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    throw new SecurityException(e);
                }
            }
        }
    }

    static void renameCompactedSsTable(Path ssTablePath) {
        Path compactionFile = ssTablePath.resolve(COMPACTED_FILE_NAME);
        Path newCompactionFile = ssTablePath.resolve(SS_TABLE_FILE_NAME + 0);
        try {
            Files.move(
                    compactionFile,
                    newCompactionFile,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING
            );
        } catch (IOException e) {
            throw new SecurityException(e);
        }
    }

    protected long getSsTableDataByteSize(Iterable<Entry<MemorySegment>> memTableEntries) {
        long ssTableDataByteSize = 0;
        long entriesCount = 0;
        for (Entry<MemorySegment> entry : memTableEntries) {
            ssTableDataByteSize += entry.key().byteSize();
            if (entry.value() != null) {
                ssTableDataByteSize += entry.value().byteSize();
            }
            entriesCount++;
        }
        memTableEntriesCount = entriesCount;
        return ssTableDataByteSize + entriesCount * Long.BYTES * 4L + Long.BYTES;
    }
}
