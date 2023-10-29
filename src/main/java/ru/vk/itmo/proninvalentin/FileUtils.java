package ru.vk.itmo.proninvalentin;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class FileUtils {
    public static void deleteFilesExcept(String directoryPath, Set<String> specifiedFileNames) {
        File directory = new File(directoryPath);

        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();

            if (files != null) {
                for (File file : files) {
                    if (file.isFile() && !specifiedFileNames.contains(file.getName())) {
                        file.delete();
                    }
                }
            }
        }
    }

    public static int parseIndexFromFileName(String fileName) {
        int i = fileName.length();
        while (i > 0 && Character.isDigit(fileName.charAt(i - 1))) {
            i--;
        }
        return Integer.parseInt(fileName.substring(i));
    }
}
