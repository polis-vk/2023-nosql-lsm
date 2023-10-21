package ru.vk.itmo.proninvalentin.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class FileUtils {
    private FileUtils() {
    }

    public static int getNewFileName(Path basePath) {
        File folder = new File(basePath.toUri());
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return 0;
        }

        int fileIndex = -1;
        for (File file : listOfFiles) {
            String fileName = file.getName();
            fileIndex = Math.max(parseIndexFromFileName(fileName), fileIndex);
        }

        return fileIndex + 1;
    }

    public static int parseIndexFromFileName(String fileName) {
        int i = fileName.length();
        while (i > 0 && Character.isDigit(fileName.charAt(i - 1))) {
            i--;
        }
        return i == fileName.length()
                ? -1
                : Integer.parseInt(fileName.substring(i));
    }

    public static void deleteFilesOldFiles(Path basePath, String filePrefix, int fileIndex) throws IOException {
        for (File file : getAllOldFilesWithPrefix(basePath, filePrefix, fileIndex)) {
            Files.delete(file.toPath());
        }
    }

    public static List<File> getAllOldFilesWithPrefix(Path basePath, String filePrefix, int actualFileIndex) {
        if (Files.notExists(basePath)) {
            return new ArrayList<>();
        }

        File folder = new File(basePath.toUri());
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return new ArrayList<>();
        }

        List<File> filesWithPrefix = new ArrayList<>();
        for (File file : listOfFiles) {
            String fileName = file.getName();
            int fileIndex = parseIndexFromFileName(fileName);
            if (file.isFile() && fileName.startsWith(filePrefix) && fileIndex < actualFileIndex) {
                filesWithPrefix.add(file);
            }
        }

        return filesWithPrefix;
    }
}
