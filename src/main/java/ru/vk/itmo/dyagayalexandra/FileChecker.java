package ru.vk.itmo.dyagayalexandra;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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

    public Map<MemorySegment, MemorySegment> checkFiles(Path basePath, Map<Path, Path> allDataPaths,
                                                        List<Path> files, Arena arena) throws IOException {
        Map<MemorySegment, MemorySegment> allFiles = new LinkedHashMap<>();
        List<Path> ssTablesPaths = new ArrayList<>();
        List<Path> ssIndexesPaths = new ArrayList<>();

        checkFile(basePath, files);

        for (Map.Entry<Path, Path> entry : allDataPaths.entrySet()) {
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
            allFiles.put(data, index);
        }

        return allFiles;
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
                indexString.indexOf(fileExtension))
        )
        ) {
            throw new NoSuchFileException("The files don't match.");
        }
    }
}
