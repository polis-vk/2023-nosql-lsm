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

    // Average L2 cache size about 1MB, so to make SSTable index fit L2 cache we should store no more
    // than 1 * 1024 * 1024 / 8 / 2 = 2 ** 16 entries
    public static final int SSTABLE_MAX_ENTRIES = (1 << 16);
    private static final String INDEX_FILENAME = "index.idx";

    private final List<MemorySegment> segmentList;

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
        DiskStorageUtils.saveMemoryTable(storagePath.resolve(newFileName), iterable);
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
            iterators.add(MemorySegmentEntryView.makeIterator(memorySegment));
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
            DiskStorageUtils.saveCompacted(newStoragePath.resolve(newFileName), views, indexSize, dataSize);
        }

        Files.write(newStoragePath.resolve(INDEX_FILENAME),
                newFiles,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING);

        final Path tempDirectory = Files.createTempDirectory(parentPath, "");
        Files.move(storagePath, tempDirectory, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        Files.move(newStoragePath, storagePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        DiskStorageUtils.cleanDirectory(tempDirectory);
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
        long left = 0;
        long right = DiskStorageUtils.recordsCount(segment) - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = DiskStorageUtils.startOfKey(segment, mid);
            long endOfKey = DiskStorageUtils.endOfKey(segment, mid);
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

        return DiskStorageUtils.tombstone(left);
    }

    private static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : DiskStorageUtils.normalize(indexOf(page, from));
        long recordIndexTo = to == null
                ? DiskStorageUtils.recordsCount(page)
                : DiskStorageUtils.normalize(indexOf(page, to));
        long recordsCount = DiskStorageUtils.recordsCount(page);

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
                MemorySegment key = DiskStorageUtils.slice(page,
                        DiskStorageUtils.startOfKey(page, index),
                        DiskStorageUtils.endOfKey(page, index));
                long startOfValue = DiskStorageUtils.startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : DiskStorageUtils.slice(page,
                                                         startOfValue,
                                                         DiskStorageUtils.endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }
}
