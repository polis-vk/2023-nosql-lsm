package ru.vk.itmo.proninvalentin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class FileUtils {
    public static String getNewFileName(Path filePath, String filePrefix) throws IOException {
        List<File> filesWithPrefix = getAllFilesWithPrefix(filePath, filePrefix);

        // Если файлов в директории с таким префиксом нету, значит новый файл будет с индексом 1;
        int fileIndex = 1;
        if (filesWithPrefix.isEmpty()) {
            return filePrefix + fileIndex;
        }

        List<String> fileNames = filesWithPrefix.stream().map(File::getName).toList();
        String fileWithMaxIndex = Collections.max(fileNames);
        fileIndex = parseIndexFromFileName(fileWithMaxIndex, filePrefix)/* + 1*/;
        return filePrefix + fileIndex;
    }

    public static List<File> getAllFilesWithPrefix(Path path, String filePrefix) throws IOException {
        if (Files.notExists(path)) {
            return new ArrayList<>();
        }

        try (Stream<Path> stream = Files.list(path)) {
            return stream
                    .filter(filePath -> !Files.isDirectory(filePath) && filePath.getFileName().startsWith(filePrefix))
                    .map(Path::toFile)
                    .toList();
        }
    }

    public static int parseIndexFromFileName(String fileName, String prefix) {
        int startIndex = prefix.length();
        String indexString = fileName.substring(startIndex);
        if (indexString.isEmpty()) {
            indexString = "0";
        }
        return Integer.parseInt(indexString);
    }
}
