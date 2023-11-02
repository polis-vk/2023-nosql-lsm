package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    public static final String SSTABLE_SUFFIX = ".sstable";
    public static final int MAX_SSTABLE_SEARCH_DEPTH = 1;
    private static final Set<StandardOpenOption> OPEN_OPTIONS = Set.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE
    );
    private static final Comparator<MemorySegment> MEM_SEGMENT_COMPARATOR = new MemSegmentComparator();
    private final Path sstablesPath;
    private final Arena arena = Arena.ofShared();
    private final Set<Path> filesSet = new ConcurrentSkipListSet<>();
    private final Map<Path, MemorySegment> fileMemSegmentMap = new ConcurrentSkipListMap<>();
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> mappings = new ConcurrentSkipListMap<>(
            MEM_SEGMENT_COMPARATOR
    );

    public InMemoryDao() {
        sstablesPath = null;
    }

    public InMemoryDao(Path basePath) throws IOException {
        sstablesPath = basePath;
        findSstables(basePath);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Queue<FileIterator> fileIterators = new ConcurrentLinkedQueue<>();
        for (Path file: filesSet) {
            try {
                if (!fileMemSegmentMap.containsKey(file)) {
                    fileMemSegmentMap.put(file, provideFileMapping(file));
                }
                FileIterator fileIterator = new FileIterator(fileMemSegmentMap.get(file), MEM_SEGMENT_COMPARATOR);
                if (from != null) {
                    fileIterator.positionate(from);
                }
                fileIterators.add(fileIterator);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        Iterator<Entry<MemorySegment>> memoryIterator = getFromMemory(from, to);
        return new FileMergeIterator(MEM_SEGMENT_COMPARATOR, fileIterators, memoryIterator, to);

    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Iterator<Entry<MemorySegment>> it = get(key, null);
        if (it.hasNext()) {
            Entry<MemorySegment> result = it.next();
            if (key.mismatch(result.key()) == -1) {
                return result;
            }
        }
        return null;
    }

    private Iterator<Entry<MemorySegment>> getFromMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return mappings.values().iterator();
        }
        if (from == null) {
            return mappings.headMap(to).values().iterator();
        }
        if (to == null) {
            return mappings.tailMap(from).values().iterator();
        }
        return mappings.subMap(from, to)
                .sequencedValues().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        mappings.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        long currentTimeMillis = System.currentTimeMillis();
        long nanoTime = System.nanoTime();
        Path filePath = sstablesPath.resolve(
                Path.of(
                    Long.toString(currentTimeMillis, Character.MAX_RADIX)
                        + Long.toString(nanoTime, Character.MAX_RADIX)
                        + SSTABLE_SUFFIX
                )
        );
        dumpToFile(filePath, currentTimeMillis, nanoTime);
        filesSet.add(filePath);
    }

    @Override
    public void close() throws IOException {
        try {
            if (!mappings.isEmpty()) {
                flush();
            }
        } finally {
            if (arena.scope().isAlive()) {
                arena.close();
            }
        }
    }

    private void findSstables(Path basePath) throws IOException {
        if (Files.exists(basePath)) {
            Files.walkFileTree(
                    basePath,
                    Set.of(),
                    MAX_SSTABLE_SEARCH_DEPTH,
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (file.toString().endsWith(SSTABLE_SUFFIX)) {
                                filesSet.add(file);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    }
            );
        }
    }

    // dumps mappings to file in format
    // timestamp in millis (long), nanoTime (long), numOfKeys (int)
    // offsetOf1stKey, offsetOf1stValue (-1 if value is null)
    // ...
    // offsetOf{numOfKeys - 1}Key, offsetOf{numOfKeys - 1}Key (-1 if value is null)
    // 1stKey, 1stValue (zero length if value is null)
    // ...
    // {numOfKeys - 1}Key, {numOfKeys - 1}Value (zero length if value is null)
    private void dumpToFile(Path path, long currentTimeMillis, long nanoTime) throws IOException {
        long size = 0;
        for (Entry<MemorySegment> entry : mappings.values()) {
            size += (entry.value() == null ? 0 : entry.value().byteSize()) + entry.key().byteSize();
        }
        size += Integer.BYTES + (2L * mappings.size() + 2) * Long.BYTES;
        try (FileChannel fc = FileChannel.open(path, OPEN_OPTIONS); Arena writeArena = Arena.ofConfined()) {
            MemorySegment mapped = fc.map(READ_WRITE, 0, size, writeArena);
            long offset = 0;
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, currentTimeMillis);
            offset += Long.BYTES;
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, nanoTime);
            offset += Long.BYTES;
            mapped.set(ValueLayout.JAVA_INT_UNALIGNED, offset, mappings.size());
            offset += Integer.BYTES;
            long offsetToWrite = Integer.BYTES + (2L * mappings.size() + 2) * Long.BYTES;
            for (Entry<MemorySegment> entry: mappings.values()) {
                mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, offsetToWrite);
                offset += Long.BYTES;
                MemorySegment.copy(
                        entry.key(), 0,
                        mapped, offsetToWrite,
                        entry.key().byteSize()
                );
                offsetToWrite += entry.key().byteSize();
                if (entry.value() == null) {
                    mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
                } else {
                    mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, offsetToWrite);
                    MemorySegment.copy(
                            entry.value(), 0,
                            mapped, offsetToWrite,
                            entry.value().byteSize()
                    );
                    offsetToWrite += entry.value().byteSize();
                }
                offset += Long.BYTES;
            }
        }
    }

    private MemorySegment provideFileMapping(Path file) throws IOException {
        try (FileChannel fc = FileChannel.open(file, StandardOpenOption.READ)) {
            return fc.map(READ_ONLY, 0, fc.size(), arena);
        }
    }
}
