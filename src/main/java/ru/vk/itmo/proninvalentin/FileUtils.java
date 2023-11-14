package ru.vk.itmo.proninvalentin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;

public final class FileUtils {
    private FileUtils() {
    }

    public static void deleteFilesExcept(String directoryPath, Set<String> specifiedFileNames) throws IOException {
        File directory = new File(directoryPath);
        if (!directory.exists() || !directory.isDirectory()) {
            return;
        }

        File[] files = directory.listFiles();
        if (files == null) {
            return;
        }

        for (File file : files) {
            if (file.isFile() && !specifiedFileNames.contains(file.getName())) {
                Files.delete(file.toPath());
            }
        }
    }

    public static String getLastFileName(List<String> existedFiles) {
        int compactionFileName = Integer.parseInt(getFileNameForCompaction(existedFiles));
        if (compactionFileName == 0) {
            return "0";
        }

        return String.valueOf(compactionFileName - 1);
    }

    public static String getNewFileName(List<String> existedFiles) {
        return existedFiles.isEmpty()
                ? String.valueOf(0)
                : String.valueOf(Integer.parseInt(existedFiles.getLast()) + 1);
    }

    public static String getFileNameForCompaction(List<String> existedFiles) {
        return existedFiles.isEmpty() || Integer.parseInt(existedFiles.getFirst()) > 0
                ? String.valueOf(0)
                : getNewFileName(existedFiles);
    }
}
