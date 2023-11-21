package ru.vk.itmo.dyagayalexandra;

import java.nio.file.Path;
import java.util.Comparator;

public class PathsComparator implements Comparator<Path> {

    private final String fileName;
    private final String fileExtension;

    public PathsComparator(String fileName, String fileExtension) {
        this.fileName = fileName;
        this.fileExtension = fileExtension;
    }

    @Override
    public int compare(Path path1, Path path2) {
        String str1 = String.valueOf(path1.getFileName());
        String str2 = String.valueOf(path2.getFileName());
        return Integer.parseInt(str2.substring(str2.indexOf(fileName) + fileName.length(),
                str2.indexOf(fileExtension)))
                - Integer.parseInt(str1.substring(str1.indexOf(fileName) + fileName.length(),
                str1.indexOf(fileExtension)));
    }
}
