package ru.vk.itmo.proninvalentin;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileUtils {
    public static String getNewFileName(Path filePath, String filePrefix) {
        var filesWithPrefix = getAllFilesWithPrefix(filePath, filePrefix);

        // Если файлов в директории с таким префиксом нету, значит новый файл будет с индексом 1;
        var fileIndex = 1;
        if (filesWithPrefix.isEmpty()) {
            return filePrefix + fileIndex;
        }

        var fileNames = filesWithPrefix.stream().map(File::getName).toList();
        var fileWithMaxIndex = Collections.max(fileNames);
        fileIndex = parseIndexFromFileName(fileWithMaxIndex, filePrefix)/* + 1*/;
        return filePrefix + fileIndex;
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

    private static int parseIndexFromFileName(String fileName, String prefix) {
        int startIndex = prefix.length();
        String indexString = fileName.substring(startIndex);
        if (indexString.isEmpty()) {
            indexString = "0";
        }
        return Integer.parseInt(indexString);
    }
}
