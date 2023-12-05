package ru.vk.itmo.reference;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Static utility functions.
 *
 * @author incubos
 */
final class FileUtils {
    private FileUtils() {
        // Only static methods
    }

    static MemorySegment mapReadOnly(
            final Arena arena,
            final Path file) throws IOException {
        try (final FileChannel channel =
                     FileChannel.open(
                             file,
                             StandardOpenOption.READ)) {
            return channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0L,
                    Files.size(file),
                    arena);
        }
    }

    static void writeFully(
            final FileChannel channel,
            final ByteBuffer buffer,
            long position) throws IOException {
        while (buffer.hasRemaining()) {
            position += channel.write(buffer, position);
        }
    }
}
