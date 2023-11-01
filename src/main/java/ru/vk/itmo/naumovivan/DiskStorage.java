package ru.vk.itmo.naumovivan;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DiskStorage {

    // Average L2 cache size about 1MB,
    // so to make SSTable index completely holdable in L2 cache
    // we should store no more than 1 * 1024 * 1024 / 8 / 2 = 2 ** 16 entries
    private static final int SSTABLE_MAX_ENTRIES = (1 << 16);
    private static final String INDEX_FILENAME = "index.idx";

    private final List<MemorySegment> segmentList;

    private static class MemorySegmentEntryView implements Comparable<MemorySegmentEntryView> {
        private final MemorySegment keyPage;
        private final long keyOffsetStart;
        private final long keyOffsetEnd;
        private final MemorySegment valuePage;
        private final long valueOffsetStart;
        private final long valueOffsetEnd;

        public static MemorySegmentEntryView fromFileRecord(final MemorySegment sstable,
                                                            final long recordIndex,
                                                            final long recordsCount) {
            final long startOfValue = startOfValue(sstable, recordIndex);
            return new MemorySegmentEntryView(sstable, startOfKey(sstable, recordIndex), normalize(startOfValue),
                                              sstable, startOfValue, endOfValue(sstable, recordIndex, recordsCount));
        }

        public static MemorySegmentEntryView fromEntry(final Entry<MemorySegment> entry) {
            final MemorySegment key = entry.key();
            final MemorySegment value = entry.value();
            return new MemorySegmentEntryView(key, 0, key.byteSize(),
                                              value, 0, value == null ? -1 : value.byteSize());
        }

        public MemorySegmentEntryView(final MemorySegment keyPage,
                                      final long keyOffsetStart,
                                      final long keyOffsetEnd,
                                      final MemorySegment valuePage,
                                      final long valueOffsetStart,
                                      final long valueOffsetEnd) {
            this.keyPage = keyPage;
            this.keyOffsetStart = keyOffsetStart;
            this.keyOffsetEnd = keyOffsetEnd;
            this.valuePage = valuePage;
            this.valueOffsetStart = valueOffsetStart;
            this.valueOffsetEnd = valueOffsetEnd;
        }

        public long keySize() {
            return keyOffsetEnd - keyOffsetStart;
        }

        public long valueSize() {
            return isValueDeleted() ? 0 : valueOffsetEnd - valueOffsetStart;
        }

        public boolean isValueDeleted() {
            return valuePage == null || isTombstone(valueOffsetStart);
        }

        public void copyKey(final MemorySegment filePage, final long fileOffset) {
            MemorySegment.copy(keyPage, keyOffsetStart, filePage, fileOffset, keySize());
        }

        public void copyValue(final MemorySegment filePage, final long fileOffset) {
            MemorySegment.copy(valuePage, valueOffsetStart, filePage, fileOffset, valueSize());
        }

        @Override
        public int compareTo(final MemorySegmentEntryView other) {
            final long mismatch = MemorySegment.mismatch(keyPage, keyOffsetStart, keyOffsetEnd,
                    other.keyPage, other.keyOffsetStart, other.keyOffsetEnd);
            if (mismatch == -1) {
                return 0;
            }

            if (mismatch == keyOffsetEnd - keyOffsetStart) {
                return -1;
            }
            if (mismatch == other.keyOffsetEnd - other.keyOffsetStart) {
                return 1;
            }

            final byte b1 = keyPage.get(ValueLayout.JAVA_BYTE, keyOffsetStart + mismatch);
            final byte b2 = other.keyPage.get(ValueLayout.JAVA_BYTE, other.keyOffsetStart + mismatch);
            return Byte.compare(b1, b2);
        }
    }

    public DiskStorage(List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
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

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, NaumovDao::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public static void save(Path storagePath, Iterable<Entry<MemorySegment>> iterable)
            throws IOException {
        final Path indexTmp = storagePath.resolve(INDEX_FILENAME + ".tmp");
        final Path indexFile = storagePath.resolve(INDEX_FILENAME);

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
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }

            // data:
            // |key0|value0|key1|value1|...
            dataOffset = indexSize;
            for (Entry<MemorySegment> entry : iterable) {
                MemorySegment key = entry.key();
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();
                if (value != null) {
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
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

    private MergeIterator<MemorySegmentEntryView> getViewMergeIterator(
            final Iterator<Entry<MemorySegment>> firstIterator) {
        final List<Iterator<MemorySegmentEntryView>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (final MemorySegment memorySegment : segmentList) {
            final long recordsCount = recordsCount(memorySegment);

            iterators.add(new Iterator<>() {
                long index;

                @Override
                public boolean hasNext() {
                    return index < recordsCount;
                }

                @Override
                public MemorySegmentEntryView next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }
                    return MemorySegmentEntryView.fromFileRecord(memorySegment, index++, recordsCount);
                }
            });
        }

        iterators.add(new Iterator<>() {
            @Override
            public boolean hasNext() {
                return firstIterator.hasNext();
            }

            @Override
            public MemorySegmentEntryView next() {
                return MemorySegmentEntryView.fromEntry(firstIterator.next());
            }
        });

        return new MergeIterator<>(iterators, MemorySegmentEntryView::compareTo) {
            @Override
            protected boolean skip(final MemorySegmentEntryView view) {
                return view.isValueDeleted();
            }
        };
    }

    public void overwriteData(final Path storagePath,
                              final Iterator<Entry<MemorySegment>> firstIterator) throws IOException {
        final Path parentPath = storagePath.getParent();
        final Path storageName = storagePath.getFileName();
        if (parentPath == null || storageName == null) {
            throw new IOException("storagePath has no parent or filename");
        }

        final List<String> newFiles = new ArrayList<>();
        final Path newStoragePath = Files.createTempDirectory(parentPath, "");

        final MergeIterator<MemorySegmentEntryView> mergeIterator = getViewMergeIterator(firstIterator);

        while (true) {
            final List<MemorySegmentEntryView> views = new ArrayList<>(SSTABLE_MAX_ENTRIES);
            long dataSize = 0;

            while (views.size() < SSTABLE_MAX_ENTRIES) {
                if (!mergeIterator.hasNext()) {
                    break;
                }
                final MemorySegmentEntryView view = mergeIterator.next();
                if (!view.isValueDeleted()) {
                    dataSize += view.keySize();
                    dataSize += view.valueSize();
                    views.add(view);
                }
            }

            if (views.isEmpty()) {
                break;
            }

            final long indexSize = (long) views.size() * 2 * Long.BYTES;

            final String newFileName = String.valueOf(newFiles.size());
            newFiles.add(newFileName);
            try (
                    FileChannel fileChannel = FileChannel.open(
                            newStoragePath.resolve(newFileName),
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.TRUNCATE_EXISTING
                    );
                    Arena writeArena = Arena.ofConfined()
            ) {
                final MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        indexSize + dataSize,
                        writeArena
                );
                long indexOffset = 0;
                long dataOffset = indexSize;
                for (final MemorySegmentEntryView view : views) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    indexOffset += Long.BYTES;
                    view.copyKey(fileSegment, dataOffset);
                    dataOffset += view.keySize();

                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    indexOffset += Long.BYTES;
                    view.copyValue(fileSegment, dataOffset);
                    dataOffset += view.valueSize();
                }
            }
        }

        Files.write(newStoragePath.resolve(INDEX_FILENAME),
                newFiles,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        final Path tempDirectory = Files.createTempDirectory(parentPath, "");
        Files.move(storagePath, tempDirectory, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        Files.move(newStoragePath, storagePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        Files.walkFileTree(tempDirectory, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public static List<MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        Path indexTmp = storagePath.resolve(INDEX_FILENAME + ".tmp");
        Path indexFile = storagePath.resolve(INDEX_FILENAME);

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
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena
                );
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

    private static boolean isTombstone(long offset) {
        return (offset & 1L << 63) != 0;
    }
}
