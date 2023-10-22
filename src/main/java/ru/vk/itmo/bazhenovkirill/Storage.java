package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class Storage {
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

        String fileName = String.valueOf(existedFiles.size());

        long dataSize = 0;
        long entriesCount = 0;
        for (Entry<MemorySegment> entry : values) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            dataSize += (value != null) ? value.byteSize() : 0;
            entriesCount++;
        }
        long indexSize = 2 * Long.BYTES * entriesCount;

        try (FileChannel channel = FileChannel.open(dataPath.resolve(fileName), WRITE_OPTIONS);
             Arena arena = Arena.ofConfined()) {
            MemorySegment segment = channel.map(FileChannel.MapMode.READ_WRITE,
                    0,
                    dataSize + indexSize,
                    arena);

            long indexOffset = 0;
            long dataOffset = indexSize;
            for (Entry<MemorySegment> entry : values) {
                MemorySegment key = entry.key();
                segment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment.copy(key, 0, segment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    segment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    segment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    MemorySegment.copy(value, 0, segment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }
        }

        existedFiles.add(fileName);
        Files.write(indexFile,
                existedFiles,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
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
                return getEntry(segments.get(i), index);
            }
        }
        return null;
    }

//    public Iterator<Entry<MemorySegment>> range(Iterator<Entry<MemorySegment>> inMemoryIterator,
//                                                MemorySegment from,
//                                                MemorySegment to) {
//        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segments.size() + 1);
//        for (MemorySegment segment : segments) {
//            iterators.add(iterator(segment, from, to));
//        }
//        iterators.add(inMemoryIterator);
//    }

//    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment segment, MemorySegment from, MemorySegment to) {
//        long size = recordsCount(segment);
//        long start = (from == null) ? 0 : normalize(indexOf(segment, from));
//        long end = (to == null) ? size : normalize(indexOf(segment, to));
//
//        return new Iterator<>() {
//            long inx = start;
//
//            @Override
//            public boolean hasNext() {
//                return inx < end;
//            }
//
//            @Override
//            public Entry<MemorySegment> next() {
//                if (!hasNext()) {
//                    throw new NoSuchElementException();
//                }
//                return getEntry(segment, inx++);
//            }
//        };
//    }

    private static long indexOf(MemorySegment segment, MemorySegment key) {
        long l = 0;
        long r = recordsCount(segment) - 1;
        while (l <= r) {
            long mid = (l + r) >>> 1;
            int resultOfComparing = comparator.compare(key, getKey(segment, mid));
            if (resultOfComparing == 0) {
                return mid;
            } else if (resultOfComparing < 0) {
                r = mid - 1;
            } else {
                l = mid + 1;
            }
        }
        return tombstone(l);
    }

    private static Path createOrMapIndexFile(Path dataPath) throws IOException {
        Path indexFile = dataPath.resolve(INDEX_FILE_NAME);

        if (!Files.exists(indexFile)) {
            Files.createFile(indexFile);
        }

        return indexFile;
    }

    private static MemorySegment getSlice(MemorySegment segment, long start, long end) {
        return segment.asSlice(start, end - start);
    }

    private static long startOfKey(MemorySegment segment, long inx) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, inx * 2 * Long.BYTES);
    }

    private static long endOfKey(MemorySegment segment, long inx) {
        return normalize(startOfValue(segment, inx));
    }

    private static MemorySegment getKey(MemorySegment segment, long inx) {
        return getSlice(segment, startOfKey(segment, inx), endOfKey(segment, inx));
    }

    private static long startOfValue(MemorySegment segment, long inx) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + inx * 2 * Long.BYTES);
    }

    private static long endOfValue(MemorySegment segment, long inx) {
        if (inx < recordsCount(segment) - 1) {
            return startOfKey(segment, inx + 1);
        }
        return segment.byteSize();
    }


    private static Entry<MemorySegment> getEntry(MemorySegment segment, long inx) {
        MemorySegment key = getKey(segment, inx);
        MemorySegment value = getValue(segment, inx);
        return new BaseEntry<>(key, value);
    }

    private static MemorySegment getValue(MemorySegment segment, long inx) {
        long start = startOfValue(segment, inx);
        if (start < 0) {
            return null;
        }
        return getSlice(segment, start, endOfValue(segment, inx));
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(long offset) {
        return offset & ~(1L << 63);
    }

    private static long recordsCount(MemorySegment segment) {
        return indexSize(segment) / (2 * Long.BYTES);
    }

    private static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }
}
