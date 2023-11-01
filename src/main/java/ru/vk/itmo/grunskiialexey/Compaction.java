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
import java.util.NavigableMap;
import java.util.NoSuchElementException;

import static ru.vk.itmo.grunskiialexey.DiskStorage.endOfKey;
import static ru.vk.itmo.grunskiialexey.DiskStorage.endOfValue;
import static ru.vk.itmo.grunskiialexey.DiskStorage.indexOf;
import static ru.vk.itmo.grunskiialexey.DiskStorage.normalize;
import static ru.vk.itmo.grunskiialexey.DiskStorage.recordsCount;
import static ru.vk.itmo.grunskiialexey.DiskStorage.slice;
import static ru.vk.itmo.grunskiialexey.DiskStorage.startOfKey;
import static ru.vk.itmo.grunskiialexey.DiskStorage.startOfValue;

public class Compaction {
    private static final String NAME_INDEX_FILE = "index.idx";
    private final List<MemorySegment> segmentList;

    public Compaction(List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from, MemorySegment to
    ) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(
                iterators,
                Comparator.comparing(Entry::key, MemorySegmentDao::compare),
                entry -> entry.value() == null
        );
    }


    public void compact(
            Path storagePath,
            NavigableMap<MemorySegment, Entry<MemorySegment>> iterable
    ) throws IOException {
        if (segmentList.isEmpty() || (segmentList.size() == 1 && iterable.isEmpty())) {
            return;
        }

        final Path indexFile = storagePath.resolve(NAME_INDEX_FILE);
        final Path newTmpCompactedFileName = storagePath.resolve("-1");
        final Path newCompactedFileName = storagePath.resolve("0");

        long startValuesOffset = 0;
        long maxOffset = 0;
        for (Iterator<Entry<MemorySegment>> it = range(iterable.values().iterator(), null, null); it.hasNext(); ) {
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
            for (Iterator<Entry<MemorySegment>> it = range(iterable.values().iterator(), null, null); it.hasNext(); ) {
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
        deleteFilesAndInMemory(Files.readAllLines(indexFile, StandardCharsets.UTF_8), storagePath);
        iterable.clear();

        Files.move(
                newTmpCompactedFileName, newCompactedFileName,
                StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING
        );
        Files.write(
                indexFile,
                List.of("0"),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    private void deleteFilesAndInMemory(List<String> existedFiles, Path storagePath) throws IOException {
        for (String fileName : existedFiles) {
            Files.delete(storagePath.resolve(fileName));
        }
        segmentList.clear();
    }

    private Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
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
}
