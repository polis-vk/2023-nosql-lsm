package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

// TODO read class 1:00
public class Compaction {
    private static final String NAME_INDEX_FILE = "index.idx";
    private final List<MemorySegment> segmentList;
    private final AtomicInteger lastFileNumber;

    public Compaction(List<MemorySegment> segmentList, AtomicInteger lastFileNumber) {
        this.segmentList = segmentList;
        this.lastFileNumber = lastFileNumber;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> inMemoryIterator,
            MemorySegment from, MemorySegment to
    ) {
        List<Iterator<Entry<MemorySegment>>> iterators = getFileIterators(from, to);
        iterators.add(inMemoryIterator);

        return new MergeIterator<>(
                iterators,
                Comparator.comparing(Entry::key, MemorySegmentDao::compare),
                entry -> entry.value() == null
        );
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        return new MergeIterator<>(
                getFileIterators(from, to),
                Comparator.comparing(Entry::key, MemorySegmentDao::compare),
                entry -> entry.value() == null
        );
    }

    private List<Iterator<Entry<MemorySegment>>> getFileIterators(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        return iterators;
    }

    public void compact(Path storagePath) throws IOException {
        if (segmentList.size() <= 1) {
            return;
        }

        final Path indexFile = storagePath.resolve(NAME_INDEX_FILE);
        final String fileName = Integer.toString(MemorySegmentDao.lastFileNumber.getAndIncrement());
        final Path newTmpCompactedFileName = storagePath.resolve(fileName + ".tmp");
        final Path newCompactedFileName = storagePath.resolve(fileName);

        long startValuesOffset = 0;
        long maxOffset = 0;
        for (Iterator<Entry<MemorySegment>> it = range(null, null); it.hasNext(); ) {
            Entry<MemorySegment> entry = it.next();
            startValuesOffset++;
            maxOffset += entry.key().byteSize() + entry.value().byteSize();
        }
        startValuesOffset *= 2 * Long.BYTES;
        maxOffset += startValuesOffset;

        try (
                FileChannel fileChannel = FileChannel.open(
                        newTmpCompactedFileName,
                        StandardOpenOption.WRITE, StandardOpenOption.READ,
                        StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            final MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, maxOffset, writeArena
            );

            long dataOffset = startValuesOffset;
            int indexOffset = 0;
            for (Iterator<Entry<MemorySegment>> it = range(null, null); it.hasNext(); ) {
                Entry<MemorySegment> entry = it.next();

                MemorySegment key = entry.key();
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                dataOffset += value.byteSize();
                indexOffset += Long.BYTES;
            }
        }

        // Delete old data
        DiskStorage.deleteFilesAndInMemory(
                segmentList,
                Files.readAllLines(indexFile, StandardCharsets.UTF_8),
                storagePath
        );

        Files.move(
                newTmpCompactedFileName, newCompactedFileName,
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
        );
        Files.write(
                indexFile,
                List.of(fileName),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordsCount = DiskStorage.recordsCount(page);
        long recordIndexFrom = from == null ? 0 : DiskStorage.normalize(DiskStorage.indexOf(page, from));
        long recordIndexTo = to == null ? recordsCount : DiskStorage.normalize(DiskStorage.indexOf(page, to));

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
                MemorySegment key = DiskStorage.slice(
                        page,
                        DiskStorage.startOfKey(page, index),
                        DiskStorage.endOfKey(page, index)
                );
                long startOfValue = DiskStorage.startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : DiskStorage.slice(
                                page,
                                startOfValue,
                                DiskStorage.endOfValue(page, index, recordsCount)
                        );
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }
}
