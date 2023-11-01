package ru.vk.itmo.solnyshkoksenia;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DiskStorage {
    private static final Comparator<MemorySegment> comparator = new MemorySegmentComparator();
    private static final String INDEX_FILE_NAME = "index.idx";
    private final Path storagePath;
    private final List<MemorySegment> segmentList;

    public DiskStorage(List<MemorySegment> segmentList, Path storagePath) {
        this.segmentList = segmentList;
        this.storagePath = storagePath;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, (e1, e2) -> comparator.compare(e1.key(), e2.key())) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public void save(Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = storagePath.resolve("index.tmp");
        final Path indexFile = storagePath.resolve(INDEX_FILE_NAME);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        String newFileName = String.valueOf(existedFiles.size());

        Entry<Long> sizes = new BaseEntry<>(0L, 0L);
        for (Entry<MemorySegment> entry : iterable) {
            sizes = countSize(entry, sizes);
        }

        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(newFileName),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = mapFile(fileChannel, sizes.key() + sizes.value(), writeArena);

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index

            // data:
            // |key0|value0|key1|value1|...
            Entry<Long> offsets = new BaseEntry<>(sizes.value(), 0L);
            for (Entry<MemorySegment> entry : iterable) {
                offsets = putEntry(fileSegment, offsets, entry);
            }
        }

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
                indexFile,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.delete(indexTmp);
    }

    public void compact(Iterable<Entry<MemorySegment>> iterable) throws IOException {
        final Path tmpFile = storagePath.resolve("tmp");
        final Path indexFile = storagePath.resolve(INDEX_FILE_NAME);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        if (existedFiles.isEmpty() && !iterable.iterator().hasNext()) {
            return; // nothing to compact
        }

        Iterator<Entry<MemorySegment>> iterator = range(iterable.iterator(), null, null);
        Iterator<Entry<MemorySegment>> iterator1 = range(iterable.iterator(), null, null);

        Entry<Long> sizes = new BaseEntry<>(0L, 0L);
        while (iterator.hasNext()) {
            sizes = countSize(iterator.next(), sizes);
        }

        try (
                FileChannel fileChannel = FileChannel.open(
                        tmpFile,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = mapFile(fileChannel, sizes.key() + sizes.value(), writeArena);

            Entry<Long> offsets = new BaseEntry<>(sizes.value(), 0L);
            while (iterator1.hasNext()) {
                offsets = putEntry(fileSegment, offsets, iterator1.next());
            }
        }

        for (String file : existedFiles) {
            Files.delete(storagePath.resolve(file));
        }

        Files.move(tmpFile, storagePath.resolve("0"), StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);

        Files.write(
                indexFile,
                List.of("0"),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        Path indexTmp = storagePath.resolve("index.tmp");
        Path indexFile = storagePath.resolve(INDEX_FILE_NAME);

        if (Files.exists(indexTmp)) {
            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } else {
            try {
                Files.createFile(indexFile);
            } catch (FileAlreadyExistsException ignored) {
                // it is ok, actually it is normal state
            }
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        List<MemorySegment> result = new ArrayList<>(existedFiles.size());
        for (String fileName : existedFiles) {
            Path file = storagePath.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = mapFile(fileChannel, Files.size(file), arena);
                result.add(fileSegment);
            }
        }

        return result;
    }

    private static long indexOf(MemorySegment segment, MemorySegment key) {
        long recordsCount = recordsCount(segment);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = startOfKey(segment, mid);
            long endOfKey = endOfKey(segment, mid);
            long mismatch = MemorySegment.mismatch(segment, startOfKey, endOfKey, key, 0, key.byteSize());
            if (mismatch == -1) {
                return mid;
            }

            if (mismatch == key.byteSize()) {
                right = mid - 1;
                continue;
            }

            if (mismatch == endOfKey - startOfKey) {
                left = mid + 1;
                continue;
            }

            int b1 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, startOfKey + mismatch));
            int b2 = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, mismatch));
            if (b1 > b2) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return tombstone(left);
    }

    private static Entry<Long> putEntry(MemorySegment fileSegment, Entry<Long> offsets, Entry<MemorySegment> entry) {
        long dataOffset = offsets.key();
        long indexOffset = offsets.value();
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
        indexOffset += Long.BYTES;

        MemorySegment key = entry.key();
        MemorySegment value = entry.value();
        MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
        dataOffset += key.byteSize();

        if (value == null) {
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
        } else {
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
            MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
            dataOffset += value.byteSize();
        }
        indexOffset += Long.BYTES;

        return new BaseEntry<>(dataOffset, indexOffset);
    }

    private static MemorySegment mapFile(FileChannel fileChannel, long size, Arena arena) throws IOException {
        return fileChannel.map(
                FileChannel.MapMode.READ_WRITE,
                0,
                size,
                arena
        );
    }

    private static Entry<Long> countSize(Entry<MemorySegment> entry, Entry<Long> sizes) {
        long dataSize = sizes.key();
        dataSize += entry.key().byteSize();
        MemorySegment value = entry.value();
        if (value != null) {
            dataSize += value.byteSize();
        }
        return new BaseEntry<>(dataSize, sizes.value() + Long.BYTES * 2);
    }

    private static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    private static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : normalize(indexOf(page, from));
        long recordIndexTo = to == null ? recordsCount(page) : normalize(indexOf(page, to));
        long recordsCount = recordsCount(page);

        return new Iterator<>() {
            long index = recordIndexFrom;

            @Override
            public boolean hasNext() {
                return index < recordIndexTo;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                MemorySegment key = slice(page, startOfKey(page, index), endOfKey(page, index));
                long startOfValue = startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : slice(page, startOfValue, endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }

    private static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    private static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    private static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    private static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    private static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(long value) {
        return value & ~(1L << 63);
    }
}
