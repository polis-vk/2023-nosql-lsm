package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.READ;

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

    public static boolean isFileNumberLessOrEq(Path pathToFile, String prefix, long num) {
        return getNumberFromFileName(pathToFile, prefix) <= num;
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
        } catch (NoSuchFileException e) {
            return 0;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * return -1 if no such file
     */
    public static MemorySegment findMemSegInListOfFiles(final List<Pair<Path, MemorySegment>> storage, int realFileIndex, String prefix) {
        for (int ind = storage.size() - 1; ind >= 0; ind--) {
            if (getNumberFromFileName(storage.get(ind).first, prefix) == (long) realFileIndex) {
                return storage.get(ind).second;
            }
        }
        return MemorySegment.NULL;
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

    public static void finalizeCompaction(Path storagePath, String SSTablePrefix) throws IOException {
        Path compactionFile = compactionFile(storagePath);
        try (Stream<Path> stream = Files.find(storagePath, 1,
                (path, attrs) -> path.getFileName().toString().startsWith(SSTablePrefix))) {
            stream.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        Path indexTmp = storagePath.resolve("index.tmp");
        Path indexFile = storagePath.resolve("index.idx");

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        boolean noData = Files.size(compactionFile) == 0;

        Files.write(
                indexTmp,
                noData ? Collections.emptyList() : Collections.singleton(SSTablePrefix + "0"),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        if (noData) {
            Files.delete(compactionFile);
        } else {
            Files.move(compactionFile, storagePath.resolve(SSTablePrefix + "0"), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static Path compactionFile(Path storagePath) {
        return storagePath.resolve("compaction");
    }

    public static List<Pair<Path, MemorySegment>> openFiles(Path dir, String prefix,
                                                            Map<Path, AtomicInteger> filesLifeStatistics, Arena arena) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Stream<Path> tabels = Files.find(dir, 1,
                (path, ignore) -> path.getFileName().toString().startsWith(prefix))) {
            final List<Path> list = new ArrayList<>(tabels.toList());
            Utils.sortByNames(list, prefix);

            List<Pair<Path, MemorySegment>> storage = new ArrayList<>();
            list.forEach(t -> {
                try (FileChannel channel = FileChannel.open(t, READ)) {
                    storage.add(new Pair<>(t,
                            channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena)));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            filesLifeStatistics.putAll(list.stream().collect(Collectors.toMap(i -> i, i -> new AtomicInteger(1))));
            return storage;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
    public static void openMemorySegment(Path filePath, List<Pair<Path, MemorySegment>> storage,
                                         Map<Path, AtomicInteger> filesLifeStatistics, Arena arenaForReading) {
        if (filePath == null) {
            return;
        }

        try (FileChannel channel = FileChannel.open(filePath, READ)) {
            storage.add(new Pair<>(filePath,
                    channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arenaForReading)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        filesLifeStatistics.put(filePath, new AtomicInteger(1));
    }

    public static void tryToDelete(Map<Path, AtomicInteger> mp, List<Pair<Path, MemorySegment>> storage) throws IOException {
        for (var kv : mp.entrySet()) {
            // TODO equal zero
            if (kv.getValue().get() <= 0) {
                int ind = getInd(storage, kv.getKey());
                Path toDelete = storage.get(ind).first;
                storage.remove(ind);
                deleteFile(toDelete);

            }
        }
    }

    public static <T, V> int getInd(List<Pair<T, V>> storage, T elem) {
        for (int i = 0; i < storage.size(); i++) {
            if (storage.get(i).first.equals(elem)) {
                return i;
            }
        }
        return -1;
    }

    public static void decrease(Path dir, Map<Path, AtomicInteger> mp, String prefix, long num) {
        mp.merge(dir.resolve(prefix + num), new AtomicInteger(-1),
                (old, dec) -> new AtomicInteger(old.decrementAndGet()));
    }

    public static void decreaseAllSmall(Path file, String prev, Map<Path, AtomicInteger> mp, long biggestToDie) {
        for (var kv : mp.entrySet()) {
            if (isFileNumberLessOrEq(kv.getKey(), prev, biggestToDie)) {
                kv.getValue().decrementAndGet();
            }
            // 157288110
            // 15728807
        }
    }

    public static void deleteFile(Path file) throws IOException {
        System.out.println("deleted! " + Files.deleteIfExists(file));
        // 157288110
        // 15728807
    }
}
