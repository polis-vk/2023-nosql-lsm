package ru.vk.itmo.solonetsarseniy;

import java.util.List;

public final class CompactionHelper {
    private CompactionHelper() {

    }

    public static String nonCompactionFileName(List<String> existedFiles) {
        return existedFiles.isEmpty()
            ? String.valueOf(0)
            : String.valueOf(Integer.parseInt(existedFiles.getLast()) + 1);
    }

    public static String compactionFileName(List<String> existedFiles) {
        return existedFiles.isEmpty() || Integer.parseInt(existedFiles.getFirst()) > 0
            ? "0"
            : nonCompactionFileName(existedFiles);
    }
}
