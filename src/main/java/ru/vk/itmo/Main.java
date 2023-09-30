package ru.vk.itmo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Main {
    public static void main(String[] args) throws IOException {
        final Path path = Path.of("file.txt");
        try(final FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ)) {
        }
    }
}
