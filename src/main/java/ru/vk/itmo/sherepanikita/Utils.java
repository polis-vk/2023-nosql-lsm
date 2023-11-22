package ru.vk.itmo.sherepanikita;

import java.nio.file.Path;

public final class Utils {

    public static Path getIndexTmp(Path path) {
        return path.resolve("index.tmp");
    }

    public static Path getIndexFile(Path path) {
        return path.resolve("index.idx");
    }

    private Utils() {

    }

}
