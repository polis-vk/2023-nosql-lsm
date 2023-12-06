package ru.vk.itmo.kovalevigor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class UtilsFiles {

    private UtilsFiles() {
    }

    public static void moveTwoFiles(
            final Path src1, final Path dst1,
            final Path src2, final Path dst2
    ) throws IOException {
        Files.move(src1, dst1);
        try {
            Files.move(src2, dst2);
        } catch (IOException e) {
            Files.move(dst1, src1);
            throw e;
        }
    }
}
