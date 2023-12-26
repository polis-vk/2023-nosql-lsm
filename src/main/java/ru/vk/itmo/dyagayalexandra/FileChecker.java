package ru.vk.itmo.dyagayalexandra;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FileChecker {

    private final String fileName;
    private final String fileExtension;
    private final String fileIndexName;
    private final CompactManager compactManager;

    public FileChecker(String fileName, String fileIndexName, String fileExtension, CompactManager compactManager) {
        this.fileName = fileName;
        this.fileIndexName = fileIndexName;
        this.fileExtension = fileExtension;
        this.compactManager = compactManager;
    }

    public List<Map.Entry<MemorySegment, MemorySegment>> checkFiles(Path basePath, Arena arena) throws IOException {
        List<Map.Entry<MemorySegment, MemorySegment>> allFiles = new ArrayList<>();
        List<Path> ssTablesPaths = new ArrayList<>();
        List<Path> ssIndexesPaths = new ArrayList<>();

        List<Path> files = getAllFiles(basePath);
        checkFile(basePath, files);

        List<Map.Entry<Path, Path>> allDataPaths = getAllDataPaths(basePath);

        for (Map.Entry<Path, Path> entry : allDataPaths) {
            ssTablesPaths.add(entry.getKey());
            ssIndexesPaths.add(entry.getValue());
        }

        ssTablesPaths.sort(new PathsComparator(fileName, fileExtension));
        ssIndexesPaths.sort(new PathsComparator(fileIndexName, fileExtension));

        if (ssTablesPaths.size() != ssIndexesPaths.size()) {
            throw new NoSuchFileException("Not all files found.");
        }

        for (int i = 0; i < ssTablesPaths.size(); i++) {
            MemorySegment data;
            MemorySegment index;
            Path dataPath = ssTablesPaths.get(i);
            Path indexPath = ssIndexesPaths.get(i);

            try (FileChannel fileChannel = FileChannel.open(dataPath)) {
                data = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena);
            }

            try (FileChannel fileChannel = FileChannel.open(indexPath)) {
                index = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena);
            }

            checkFileMatch(dataPath, indexPath);
            allFiles.add(new AbstractMap.SimpleEntry<>(data, index));
        }

        return allFiles;
    }

    public List<Map.Entry<Path, Path>> getAllDataPaths(Path basePath) throws IOException {
        List<Path> files = getAllFiles(basePath);
        List<Path> ssTablesPaths = new ArrayList<>();
        List<Path> ssIndexesPaths = new ArrayList<>();
        List<Map.Entry<Path, Path>> filePathsMap = new ArrayList<>();
        for (Path file : files) {
            if (String.valueOf(file.getFileName()).startsWith(fileName)) {
                ssTablesPaths.add(file);
            }

            if (String.valueOf(file.getFileName()).startsWith(fileIndexName)) {
                ssIndexesPaths.add(file);
            }
        }

        ssTablesPaths.sort(new PathsComparator(fileName, fileExtension));
        ssIndexesPaths.sort(new PathsComparator(fileIndexName, fileExtension));

        int size = ssTablesPaths.size();
        for (int i = 0; i < size; i++) {
            filePathsMap.add(new AbstractMap.SimpleEntry<>(ssTablesPaths.get(i), ssIndexesPaths.get(i)));
        }

        return filePathsMap;
    }

    private void checkFile(Path basePath, List<Path> files) throws IOException {
        for (Path file : files) {
            if (compactManager.deleteTempFile(basePath, file, files)) {
                break;
            }

            if (compactManager.clearIfCompactFileExists(basePath, file, files)) {
                break;
            }
        }
    }

    private void checkFileMatch(Path data, Path index) throws IOException {
        String dataString = data.toString();
        String indexString = index.toString();
        if (Integer.parseInt(dataString.substring(
                dataString.indexOf(fileName) + fileName.length(),
                dataString.indexOf(fileExtension)))
                != Integer.parseInt(indexString.substring(
                indexString.indexOf(fileIndexName) + fileIndexName.length(),
                indexString.indexOf(fileExtension)))) {
            throw new NoSuchFileException("The files don't match.");
        }
    }

    private List<Path> getAllFiles(Path basePath) throws IOException {
        List<Path> files = new ArrayList<>();
        if (!Files.exists(basePath)) {
            Files.createDirectory(basePath);
        }

        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(basePath)) {
            for (Path path : directoryStream) {
                files.add(path);
            }
        }

        return files;
    }
}
