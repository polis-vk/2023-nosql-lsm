package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class Storage {

    private static final AtomicInteger SSTABLE_ID = new AtomicInteger();
    private static final MemorySegmentComparator comparator = new MemorySegmentComparator();
    private static final String INDEX_FILE_NAME = "index.db";
    private static final Set<StandardOpenOption> WRITE_OPTIONS = Set.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ
    );
    private final List<MemorySegment> segments;

    public Storage(List<MemorySegment> segments) {
        this.segments = segments;
    }

    /*
        dataFile:
        index -> |key0_offset|value0_offset|...|keyn_offset|valuen_offset|
        data -> |ke0|value0|...|keyn|valuen|
        index, data in one file
     */
    public static void save(Path dataPath, Iterable<Entry<MemorySegment>> values) throws IOException {
        Path indexFile = createOrMapIndexFile(dataPath);
        List<String> existedFiles = Files.readAllLines(indexFile);

        String fileName = String.valueOf(SSTABLE_ID.incrementAndGet());
        writeDataToSSTable(dataPath.resolve(fileName), values);

        existedFiles.add(fileName);
        Files.write(indexFile,
                existedFiles,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    public boolean compact(Path dataPath, Iterable<Entry<MemorySegment>> values) throws IOException {
        Path indexFile = createOrMapIndexFile(dataPath);
        List<String> existedFiles = Files.readAllLines(indexFile);
        if (existedFiles.isEmpty()) {
            return false;
        }

        Path compactionTmpFile = getCompactionPath(dataPath);

        compactData(compactionTmpFile, values);
        finalizeCompaction(dataPath, existedFiles);
        return true;
    }

    private void finalizeCompaction(Path dataPath, List<String> existedFiles) throws IOException {
        Path compactionTmpFile = getCompactionPath(dataPath);
        if (Files.size(compactionTmpFile) == 0) {
            return;
        }

        for (String name : existedFiles) {
            Files.deleteIfExists(dataPath.resolve(name));
        }

        Path indexFile = createOrMapIndexFile(dataPath);
        String newSSTableName = String.valueOf(SSTABLE_ID.incrementAndGet());
        Files.write(
                indexFile,
                Collections.singleton(newSSTableName),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(compactionTmpFile, dataPath.resolve(newSSTableName), StandardCopyOption.ATOMIC_MOVE);
    }

    private Path getCompactionPath(Path dataPath) {
        return dataPath.resolve("compaction.tmp");
    }

    private void compactData(Path ssTablePath, Iterable<Entry<MemorySegment>> values) throws IOException {
        long dataSize = 0;
        long entriesCount = 0;

        for (Entry<MemorySegment> entry : values) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            dataSize += (value == null) ? 0 : value.byteSize();
            entriesCount++;
        }
        long indexSize = 2 * Long.BYTES * entriesCount;

        try (FileChannel channel = FileChannel.open(ssTablePath, WRITE_OPTIONS);
             Arena arena = Arena.ofConfined()) {
            MemorySegment segment = channel.map(FileChannel.MapMode.READ_WRITE,
                    0,
                    dataSize + indexSize,
                    arena);

            Offset offset = new Offset(indexSize, 0);
            for (Entry<MemorySegment> entry : values) {
                writeEntry(entry, segment, offset);

            }
        }
    }

    private static void writeDataToSSTable(Path ssTablePath, Iterable<Entry<MemorySegment>> values) throws IOException {
        long dataSize = 0;
        long entriesCount = 0;
        for (Entry<MemorySegment> entry : values) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            dataSize += (value == null) ? 0 : value.byteSize();
            entriesCount++;
        }
        long indexSize = 2 * Long.BYTES * entriesCount;

        try (FileChannel channel = FileChannel.open(ssTablePath, WRITE_OPTIONS);
             Arena arena = Arena.ofConfined()) {
            MemorySegment segment = channel.map(FileChannel.MapMode.READ_WRITE,
                    0,
                    dataSize + indexSize,
                    arena);

            Offset offset = new Offset(indexSize, 0);
            for (Entry<MemorySegment> entry : values) {
                writeEntry(entry, segment, offset);
            }
        }
    }

    private static void writeEntry(Entry<MemorySegment> entry, MemorySegment segment, Offset offset) {
        MemorySegment key = entry.key();
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset.index, offset.data);
        MemorySegment.copy(key, 0, segment, offset.data, key.byteSize());
        offset.data += key.byteSize();
        offset.index += Long.BYTES;

        MemorySegment value = entry.value();
        if (value == null) {
            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset.index, MemorySegmentUtils.tombstone(offset.data));
        } else {
            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset.index, offset.data);
            MemorySegment.copy(value, 0, segment, offset.data, value.byteSize());
            offset.data += value.byteSize();
        }
        offset.index += Long.BYTES;
    }

    public static List<MemorySegment> loadData(Path dataPath, Arena arena) throws IOException {
        Path indexFile = createOrMapIndexFile(dataPath);

        List<String> existedFiles = Files.readAllLines(indexFile);
        List<MemorySegment> segments = new ArrayList<>(existedFiles.size());
        for (String name : existedFiles) {
            Path dataFile = dataPath.resolve(name);
            try (FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment segment = channel.map(FileChannel.MapMode.READ_WRITE, 0, channel.size(), arena);
                segments.add(segment);
            }
        }

        return segments;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        for (int i = segments.size() - 1; i >= 0; --i) {
            long index = indexOf(segments.get(i), key);
            if (index >= 0) {
                Entry<MemorySegment> entry = MemorySegmentUtils.getEntry(segments.get(i), index);
                return entry.value() == null ? null : entry;
            }
        }
        return null;
    }

    public Iterator<Entry<MemorySegment>> range(Iterator<Entry<MemorySegment>> inMemoryIterator,
                                                MemorySegment from,
                                                MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segments.size() + 1);
        for (MemorySegment segment : segments) {
            iterators.add(iterator(segment, from, to));
        }
        iterators.add(inMemoryIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, comparator)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment segment,
                                                           MemorySegment from,
                                                           MemorySegment to) {
        long size = MemorySegmentUtils.recordsCount(segment);
        long start = (from == null) ? 0 : MemorySegmentUtils.normalize(indexOf(segment, from));
        long end = (to == null) ? size : MemorySegmentUtils.normalize(indexOf(segment, to));

        return new Iterator<>() {
            long inx = start;

            @Override
            public boolean hasNext() {
                return inx < end;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return MemorySegmentUtils.getEntry(segment, inx++);
            }
        };
    }

    private static long indexOf(MemorySegment segment, MemorySegment key) {
        long l = 0;
        long r = MemorySegmentUtils.recordsCount(segment) - 1;
        while (l <= r) {
            long mid = (l + r) >>> 1;
            int resultOfComparing = comparator.compare(key, MemorySegmentUtils.getKey(segment, mid));
            if (resultOfComparing == 0) {
                return mid;
            } else if (resultOfComparing < 0) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        return MemorySegmentUtils.tombstone(l);
    }

    private static Path createOrMapIndexFile(Path dataPath) throws IOException {
        Path indexFile = dataPath.resolve(INDEX_FILE_NAME);

        if (!Files.exists(indexFile)) {
            Files.createFile(indexFile);
        }

        return indexFile;
    }
}
