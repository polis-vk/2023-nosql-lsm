package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static ru.vk.itmo.prokopyevnikita.StorageAdditionalFunctionality.binarySearchUpperBoundOrEquals;
import static ru.vk.itmo.prokopyevnikita.StorageAdditionalFunctionality.getEntryByIndex;
import static ru.vk.itmo.prokopyevnikita.StorageAdditionalFunctionality.saveEntrySegment;

public final class Storage implements Closeable {

    public static final long FILE_PREFIX = Long.BYTES;
    private static final String DB_PREFIX = "data";
    private static final String DB_EXTENSION = ".db";
    private static final String INDEX_FILE = "index.idx";
    private static final String INDEX_TMP_FILE = "index.tmp";
    private final Arena arena;
    private final List<MemorySegment> ssTables;

    private final boolean isCompacted;

    private Storage(Arena arena, List<MemorySegment> ssTables, boolean isCompacted) {
        this.arena = arena;
        this.ssTables = ssTables;
        this.isCompacted = isCompacted;
    }

    public static Storage load(Config config) throws IOException {
        Path path = config.basePath();

        List<MemorySegment> ssTables = new ArrayList<>();
        Arena arena = Arena.ofShared();

        Path indexFile = path.resolve(INDEX_FILE);

        if (Files.exists(indexFile)) {
            List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
            for (String fileName : existedFiles) {
                Path file = path.resolve(fileName);
                try (FileChannel fileChannel = FileChannel.open(
                        file,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                )) {
                    MemorySegment fileSegment = fileChannel.map(
                            FileChannel.MapMode.READ_WRITE,
                            0,
                            Files.size(file),
                            arena
                    );
                    ssTables.add(fileSegment);
                }
            }
            Collections.reverse(ssTables);
        }
        boolean isCompacted = ssTables.size() <= 1;
        return new Storage(arena, ssTables, isCompacted);
    }

    public static void save(Config config, Collection<Entry<MemorySegment>> entries, Storage storage)
            throws IOException {
        if (storage.arena.scope().isAlive()) {
            throw new IllegalStateException("Previous arena is alive");
        }

        if (entries.isEmpty()) {
            return;
        }

        Path path = config.basePath();
        Path indexFile = path.resolve(INDEX_FILE);

        if (!Files.exists(indexFile)) {
            Files.createFile(indexFile);
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        int nextSSTable = existedFiles.size();
        String nextSSTableName = DB_PREFIX + nextSSTable + DB_EXTENSION;
        Path tmpPath = config.basePath().resolve(nextSSTableName);

        saveByPath(tmpPath, entries::iterator);

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(nextSSTableName);
        Path indexTmp = path.resolve(INDEX_TMP_FILE);
        Files.write(
                indexTmp,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.deleteIfExists(indexFile);

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
    }

    private static void saveByPath(Path path, IterableData entries) throws IOException {
        if (Files.exists(path)) {
            throw new IllegalStateException("File already exists: " + path);
        }

        long entriesCount = 0;
        long entriesSize = 0;
        for (Entry<MemorySegment> entry : entries) {
            entriesSize += 2 * Long.BYTES + entry.key().byteSize()
                    + (entry.value() == null ? 0 : entry.value().byteSize());
            entriesCount++;
        }

        long indicesSize = entriesCount * Long.BYTES;
        long sizeOfNewSSTable = FILE_PREFIX + indicesSize + entriesSize;

        try (Arena arenaSave = Arena.ofConfined();
             FileChannel channel = FileChannel.open(
                     path,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.CREATE)
        ) {

            MemorySegment newSSTable = channel.map(FileChannel.MapMode.READ_WRITE, 0, sizeOfNewSSTable, arenaSave);

            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entriesCount);

            long offsetIndex = FILE_PREFIX;
            long offsetData = indicesSize + FILE_PREFIX;
            for (Entry<MemorySegment> entry : entries) {
                newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, offsetData);
                offsetIndex += Long.BYTES;

                offsetData = saveEntrySegment(newSSTable, entry, offsetData);
            }

        }
    }

    public static void compact(Config config, IterableData entries) throws IOException {
        Path path = config.basePath();
        Path tmpCompactedPath = path.resolve("compacted" + DB_EXTENSION);
        saveByPath(tmpCompactedPath, entries);

        try (Stream<Path> stream = Files.find(path, 1, (p, a) -> p.getFileName().toString().startsWith(DB_PREFIX))) {
            stream.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        Path indexTmp = path.resolve(INDEX_TMP_FILE);
        Path indexFile = path.resolve(INDEX_FILE);

        Files.deleteIfExists(indexTmp);
        Files.deleteIfExists(indexFile);

        boolean noData = Files.size(tmpCompactedPath) == 0;

        Files.write(
                indexTmp,
                noData ? Collections.emptyList() : Collections.singleton(DB_PREFIX + "0" + DB_EXTENSION),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);

        if (noData) {
            Files.delete(tmpCompactedPath);
        } else {
            Files.move(tmpCompactedPath, path.resolve(DB_PREFIX + 0 + DB_EXTENSION), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    public Iterator<Entry<MemorySegment>> iterateThroughSSTable(
            MemorySegment ssTable,
            MemorySegment from,
            MemorySegment to
    ) {
        long left = binarySearchUpperBoundOrEquals(ssTable, from);
        long right = binarySearchUpperBoundOrEquals(ssTable, to);

        return new Iterator<>() {
            long current = left;

            @Override
            public boolean hasNext() {
                return current < right;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No next element");
                }
                return getEntryByIndex(ssTable, current++);
            }
        };
    }

    public Iterator<Entry<MemorySegment>> getIterator(
            MemorySegment from,
            MemorySegment to,
            Iterator<Entry<MemorySegment>> memoryIterator
    ) {
        List<OrderedPeekIterator<Entry<MemorySegment>>> peekIterators = new ArrayList<>();
        peekIterators.add(new OrderedPeekIteratorImpl(0, memoryIterator));
        int order = 1;
        for (MemorySegment sstable : ssTables) {
            Iterator<Entry<MemorySegment>> iterator = iterateThroughSSTable(sstable, from, to);
            peekIterators.add(new OrderedPeekIteratorImpl(order, iterator));
            order++;
        }
        return new MergeSkipNullValuesIterator(peekIterators);
    }

    @Override
    public void close() {
        if (arena.scope().isAlive()) {
            arena.close();
        }
    }

    public boolean isCompacted() {
        return isCompacted;
    }

    @FunctionalInterface
    public interface IterableData extends Iterable<Entry<MemorySegment>> {
        @Override
        Iterator<Entry<MemorySegment>> iterator();
    }
}
