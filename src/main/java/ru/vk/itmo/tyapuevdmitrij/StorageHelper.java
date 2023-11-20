package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class StorageHelper {
    static final String SS_TABLE_FILE_NAME = "ssTable";
    static final String TEMP_SS_TABLE_FILE_NAME = "tempSsTable";

    static final String COMPACTED_FILE_NAME = "compact";
    protected long memTableEntriesCount;

    public int findSsTablesQuantity(Path ssTablePath) {
        File[] files = getDirectoryFiles(ssTablePath);
        if (files.length == 0) {
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

    public void deleteOldSsTables(Path ssTablePath) {
        File[] files = getDirectoryFiles(ssTablePath);
        for (File file : files) {
            if (file.getName().contains(SS_TABLE_FILE_NAME)) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    throw new FilesException("Can't delete file " + file.toPath(), e);
                }
            }
        }
    }

    private File[] getDirectoryFiles(Path ssTablePath) {
        File directory = new File(ssTablePath.toUri());
        if (!directory.exists() || !directory.isDirectory()) {
            return new File[0];
        }
        return directory.listFiles();
    }

    public void renameCompactedSsTable(Path ssTablePath) {
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
            throw new FilesException("Can't rename file", e);
        }
    }

    public long getSsTableDataByteSize(Iterable<Entry<MemorySegment>> memTableEntries) {
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
