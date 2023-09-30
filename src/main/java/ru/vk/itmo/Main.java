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
        final String content = "Hello, world!";
        try (final BufferedWriter out = Files.newBufferedWriter(path)) {
            out.write(content);
        }
        final byte[] contentBytes = content.getBytes();

        try (final FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.READ)) {
            final MemorySegment segment =
                    channel.map(FileChannel.MapMode.READ_WRITE, channel.size(), 16, Arena.ofAuto());
            segment.set(ValueLayout.JAVA_BYTE, 0, (byte) 1);
            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, 2, 213123);
        }
    }
}
