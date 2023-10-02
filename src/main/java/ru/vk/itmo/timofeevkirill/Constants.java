package ru.vk.itmo.timofeevkirill;

import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {
    public final static String FILE_NAME = "SS_TABLE";
    public final static Set<OpenOption> WRITE_OPTIONS = new HashSet<>(Arrays.asList(
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
    ));
    public final static OpenOption READ_OPTIONS = StandardOpenOption.READ;
}
