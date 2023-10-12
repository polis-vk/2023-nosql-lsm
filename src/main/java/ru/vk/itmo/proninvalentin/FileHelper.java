package ru.vk.itmo.proninvalentin;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileHelper {
    public static String getNewFileName(Path path, String filePrefix) {
        var filesWithPrefix = getAllFilesWithPrefix(path, filePrefix);

        // Если файлов в директории с таким префиксом нету, значит новый файл будет с индексом 1;
        var fileIndex = 1;
        if (filesWithPrefix.isEmpty()) {
            return filePrefix + fileIndex;
        }

        var fileWithMaxIndex = Collections.max(filesWithPrefix);
        fileIndex = parseIndexFromFileName(fileWithMaxIndex, filePrefix) + 1;
        return filePrefix + fileIndex;
    }

    private static List<String> getAllFilesWithPrefix(Path path, String filePrefix) {
        if (Files.notExists(path)) {
            return new ArrayList<>();
        }

        File folder = new File(path.toUri());
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return new ArrayList<>();
        }

        List<String> fileNames = new ArrayList<>();
        for (File file : listOfFiles) {
            if (file.isFile() && file.getName().startsWith(filePrefix)) {
                fileNames.add(file.getName());
            }
        }

        return fileNames;
    }

    private static int parseIndexFromFileName(String fileName, String prefix) {
        int startIndex = prefix.length();
        String indexString = fileName.substring(startIndex);
        if (indexString.isEmpty()) {
            indexString = "0";
        }
        return Integer.parseInt(indexString);
    }
}
