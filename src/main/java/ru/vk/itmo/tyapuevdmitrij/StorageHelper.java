package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public abstract class StorageHelper {

    protected static int findSsTablesQuantity(Path ssTablePath) {
        File dir = new File(ssTablePath.toUri());
        File[] files = dir.listFiles();
        if (files == null) {
            return 0;
        }
        long countSsTables = Arrays.stream(files)
                .filter(file -> file.isFile() && file.getName().contains(Storage.SS_TABLE_FILE_NAME))
                .count();
        return (int) countSsTables;
    }

    protected static void deleteOldSsTables(Path ssTablePath, int ssTablesQuantity) {
        File directory = new File(ssTablePath.toUri());
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.getName().contains(Storage.SS_TABLE_FILE_NAME + ssTablesQuantity)) {
                        try {
                            Files.delete(file.toPath());
                        } catch (IOException e) {
                            throw new SecurityException(e);
                        }
                    }
                }
            }
        }
    }

    protected static void renameCompactedSsTable(Path ssTablePath) {
        File directory = new File(ssTablePath.toUri());
        boolean renamed = false;
        if (directory.exists() && directory.isDirectory()) {
            File[] remainingFiles = directory.listFiles();
            if (remainingFiles != null && remainingFiles.length == 1) {
                File remainingFile = remainingFiles[0];
                String newFilePath = remainingFile.getParent() + File.separator + Storage.SS_TABLE_FILE_NAME + 0;
                 renamed = remainingFile.renameTo(new File(newFilePath));
            }
        }
        if (!renamed) {
            throw new SecurityException();
        }
    }

    protected static long getSsTableDataByteSize(Iterable<Entry<MemorySegment>> memTableEntries) {
        long ssTableDataByteSize = 0;
        long entriesCount = 0;
        for (Entry<MemorySegment> entry : memTableEntries) {
            ssTableDataByteSize += entry.key().byteSize();
            if (entry.value() != null) {
                ssTableDataByteSize += entry.value().byteSize();
            }
            entriesCount++;
        }
        Storage.memTableEntriesSize = entriesCount;
        return ssTableDataByteSize + entriesCount * Long.BYTES * 4L + Long.BYTES;
    }
}
