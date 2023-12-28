package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public final class Utils {
    private Utils() {

    }

    public static long dumpLong(MemorySegment mapped, long value, long offset) {
        mapped.set(ValueLayout.JAVA_LONG, offset, value);
        return offset + Long.BYTES;
    }

    public static long dumpSegment(MemorySegment mapped, MemorySegment data, long offset) {
        MemorySegment.copy(data, 0, mapped, offset, data.byteSize());
        return offset + data.byteSize();
    }

    public static long getNumberFromFileName(Path pathToFile, String prefix) {
        return Long.parseLong(pathToFile.getFileName().toString().substring(prefix.length()));
    }

    public static void sortByNames(List<Path> l, String prefix) {
        l.sort(Comparator.comparingLong(s -> getNumberFromFileName(s, prefix)));
    }

    public static long getMaxNumberOfFile(Path dir, String prefix) {
        try (Stream<Path> tabels = Files.find(dir, 1,
                (path, ignore) -> path.getFileName().toString().startsWith(prefix))) {
            final List<Path> list = tabels.toList();
            long maxi = 0;
            for (Path p : list) {
                if (getNumberFromFileName(p, prefix) > maxi) {
                    maxi = getNumberFromFileName(p, prefix);
                }
            }
            return maxi;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static void deleteFiles(List<Path> files) throws IOException {
        for (Path file : files) {
            Files.deleteIfExists(file);
        }
        files.clear();
    }

    public static long rightByteSize(Entry<MemorySegment> memSeg) {
        if (memSeg.value() == null) {
            return -1;
        }
        return memSeg.value().byteSize();
    }

    public static MemorySegment getValueOrNull(Entry<MemorySegment> kv) {
        MemorySegment value = kv.value();
        if (kv.value() == null) {
            value = MemorySegment.NULL;
        }
        return value;
    }
}
