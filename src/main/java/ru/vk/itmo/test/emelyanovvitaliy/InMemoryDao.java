package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Path sstablesPath;
    private final Arena arena = Arena.ofShared();
    private final Set<Path> filesSet = new ConcurrentSkipListSet<>();

    public static final Comparator<MemorySegment> comparator = (o1, o2) -> {
        if (o1 == o2) {
            return 0;
        }
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == o1.byteSize()) {
            return -1;
        }
        if (mismatch == o2.byteSize()) {
            return 1;
        }
        return Byte.compare(o1.get(ValueLayout.JAVA_BYTE, mismatch), o2.get(ValueLayout.JAVA_BYTE, mismatch));
    };
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> mappings = new ConcurrentSkipListMap<>(
            comparator
    );

    public InMemoryDao() {
        sstablesPath = null;
    }

    public InMemoryDao(Path basePath) throws IOException {
        sstablesPath = basePath;
        if (Files.exists(basePath)) {
            Files.walkFileTree(
                    basePath,
                    Set.of(),
                    1,
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (file.toString().endsWith(".sstable")) {
                                filesSet.add(file);
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    }
            );
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        Queue<FileIterator> fileIterators = new ConcurrentLinkedQueue<>();
        for (Path file: filesSet) {
            try {
                FileIterator fileIterator = new FileIterator(file, comparator);
                if (from != null) {
                    fileIterator.positionate(from);
                }
                fileIterators.add(fileIterator);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        Iterator<Entry<MemorySegment>> memoryIterator = getFromMemory(from, to);
        return new FileMergeIterator(comparator, fileIterators, memoryIterator, to);

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

    public Iterator<Entry<MemorySegment>> getFromMemory(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return mappings.values().iterator();
        } else if (from == null) {
            return mappings.headMap(to).values().iterator();
        } else if (to == null) {
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
        Path filePath = sstablesPath.resolve(
                Path.of(
                    Long.toString(System.currentTimeMillis(), Character.MAX_RADIX) +
                    Long.toString(System.nanoTime(), Character.MAX_RADIX)   +
                    ".sstable"
                )
        );
        dumpToFile(filePath);
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


    // dumps mappings to file in format
    // timestamp in millis (long), numOfKeys (int)
    // offsetOf1stKey, offsetOf1stValue (-1 if value is null)
    // ...
    // offsetOf{numOfKeys - 1}Key, offsetOf{numOfKeys - 1}Key (-1 if value is null)
    // 1stKey, 1stValue (zero length if value is null)
    // ...
    // {numOfKeys - 1}Key, {numOfKeys - 1}Value (zero length if value is null)
    private void dumpToFile(Path path) throws IOException {
        long size = 0;
        Set<StandardOpenOption> openOptions =
                Set.of(
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                );
        for (Entry<MemorySegment> entry : mappings.values()) {
            size += (entry.value() == null ? 0 : entry.value().byteSize()) + entry.key().byteSize();
        }
        size += Integer.BYTES + (2L * mappings.size() + 2) * Long.BYTES;
        try (FileChannel fc = FileChannel.open(path, openOptions); Arena writeArena = Arena.ofConfined()) {
            MemorySegment mapped = fc.map(READ_WRITE, 0, size, writeArena);
            long offset = 0;
            long offsetToWrite = 0;
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, System.currentTimeMillis());
            offset += Long.BYTES;
            mapped.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, System.nanoTime());
            offset += Long.BYTES;
            mapped.set(ValueLayout.JAVA_INT_UNALIGNED, offset, mappings.size());
            offset += Integer.BYTES;
            offsetToWrite += Integer.BYTES + (2L * mappings.size() + 2) * Long.BYTES;
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
}
