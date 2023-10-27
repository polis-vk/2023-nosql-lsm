package ru.vk.itmo.timofeevkirill;

import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class Constants {
    public static final String FILE_NAME_PREFIX = "SS_TABLE_";
    public static final String FILE_NAME_CONFIG = "_CONFIG";
    public static final Set<OpenOption> WRITE_OPTIONS = new HashSet<>(Arrays.asList(
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
    ));
    public static final OpenOption READ_OPTIONS = StandardOpenOption.READ;
    public static final long PAGE_SIZE = 4096;

    private Constants() {
        throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
    }
}
