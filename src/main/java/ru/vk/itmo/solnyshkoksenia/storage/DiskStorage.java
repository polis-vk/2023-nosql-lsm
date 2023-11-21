package ru.vk.itmo.solnyshkoksenia.storage;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solnyshkoksenia.MemorySegmentComparator;
import ru.vk.itmo.solnyshkoksenia.MergeIterator;

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
    private static final StorageUtils utils = new StorageUtils();
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

        long dataSize = 0;
        long count = 0;
        for (Entry<MemorySegment> entry : iterable) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(newFileName),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = utils.mapFile(fileChannel, indexSize + dataSize, writeArena);

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index

            // data:
            // |key0|value0|key1|value1|...
            Entry<Long> offsets = new BaseEntry<>(indexSize, 0L);
            for (Entry<MemorySegment> entry : iterable) {
                offsets = utils.putEntry(fileSegment, offsets, entry);
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

        long dataSize = 0;
        long indexSize = 0;
        while (iterator.hasNext()) {
            indexSize += Long.BYTES * 2;
            Entry<MemorySegment> entry = iterator.next();
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
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
            MemorySegment fileSegment = utils.mapFile(fileChannel, indexSize + dataSize, writeArena);

            Entry<Long> offsets = new BaseEntry<>(indexSize, 0L);
            while (iterator1.hasNext()) {
                offsets = utils.putEntry(fileSegment, offsets, iterator1.next());
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
                MemorySegment fileSegment = utils.mapFile(fileChannel, Files.size(file), arena);
                result.add(fileSegment);
            }
        }

        return result;
    }

    private static long indexOf(MemorySegment segment, MemorySegment key) {
        long recordsCount = utils.recordsCount(segment);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = utils.startOfKey(segment, mid);
            long endOfKey = utils.endOfKey(segment, mid);
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

        return utils.tombstone(left);
    }

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : utils.normalize(indexOf(page, from));
        long recordIndexTo = to == null ? utils.recordsCount(page) : utils.normalize(indexOf(page, to));
        long recordsCount = utils.recordsCount(page);

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
                MemorySegment key = utils.slice(page, utils.startOfKey(page, index), utils.endOfKey(page, index));
                long startOfValue = utils.startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : utils.slice(page, startOfValue, utils.endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }
}
