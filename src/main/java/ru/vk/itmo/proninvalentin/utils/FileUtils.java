package ru.vk.itmo.proninvalentin.utils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class FileUtils {
    private FileUtils() {
    }

    public static String getNewFileName(Path filePath, String filePrefix) {
        int fileIndex = -1;
        for (File file : getAllFilesWithPrefix(filePath, filePrefix)) {
            String fileName = file.getName();
            fileIndex = Math.max(parseIndexFromFileName(fileName), fileIndex);
        }

        return filePrefix + (fileIndex + 1);
    }

    public static List<File> getAllFilesWithPrefix(Path path, String filePrefix) {
        if (Files.notExists(path)) {
            return new ArrayList<>();
        }

        File folder = new File(path.toUri());
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return new ArrayList<>();
        }

        List<File> filesWithPrefix = new ArrayList<>();
        for (File file : listOfFiles) {
            if (file.isFile() && file.getName().startsWith(filePrefix)) {
                filesWithPrefix.add(file);
            }
        }

        return filesWithPrefix;
    }

    public static int parseIndexFromFileName(String fileName) {
        int i = fileName.length();
        while (i > 0 && Character.isDigit(fileName.charAt(i - 1))) {
            i--;
        }
        return Integer.parseInt(fileName.substring(i));
    }
}
