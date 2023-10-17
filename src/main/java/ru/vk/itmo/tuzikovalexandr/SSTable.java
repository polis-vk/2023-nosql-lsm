package ru.vk.itmo.tuzikovalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
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
import java.time.Instant;
import java.util.*;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class SSTable {

    private static final Set<OpenOption> openOptions = Set.of(
            StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
    );

    private final Path basePath;
    private final Arena readArena;
    private static final String FILE_PREFIX = "data_";
    private static final String OFFSET_PREFIX = "offset_";
    private static final String INDEX_FILE = "index";
    private final Comparator<MemorySegment> comparator = MemorySegmentComparator::compare;
    private final List<BaseEntry<MemorySegment>> files = new ArrayList<>();
    private final List<Long> listIndex;

    public SSTable(Config config) throws IOException {
        this.basePath = config.basePath();

        listIndex = getAllIndex();

        readArena = Arena.ofConfined();

        if (listIndex == null) {
            return;
        }

        for (Long index : listIndex) {
            Path offsetFullPath = basePath.resolve(OFFSET_PREFIX + index);
            MemorySegment readSegmentOffset = FileChannel.open(offsetFullPath, StandardOpenOption.READ)
                    .map(READ_ONLY, 0, Files.size(offsetFullPath), readArena);

            Path fileFullPath = basePath.resolve(FILE_PREFIX + index);
            MemorySegment readSegmentData = FileChannel.open(fileFullPath, StandardOpenOption.READ)
                    .map(READ_ONLY, 0, Files.size(fileFullPath), readArena);

            files.add(new BaseEntry<>(readSegmentOffset, readSegmentData));
        }
    }

    public void saveMemData(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (!readArena.scope().isAlive()) {
            return;
        }

        readArena.close();

        long[] offsets = new long[entries.size() * 2];

        long offsetData = 0;
        long memorySize = 0;
        for (Entry<MemorySegment> entry : entries) {
            memorySize += entry.key().byteSize();
            if (entry.value() != null) {
                memorySize += entry.value().byteSize();
            }
        }
        int index = 0;

        long instantNow = Instant.now().toEpochMilli();
        try (FileChannel fcData = FileChannel.open(basePath.resolve(FILE_PREFIX + instantNow), openOptions);
             FileChannel fcOffset = FileChannel.open(basePath.resolve(OFFSET_PREFIX + instantNow), openOptions);
             FileChannel fcIndex = FileChannel.open(basePath.resolve(INDEX_FILE), openOptions)) {

            MemorySegment writeSegmentData = fcData.map(READ_WRITE, 0, memorySize, Arena.ofConfined());
            MemorySegment writeSegmentOffset = fcOffset.map(READ_WRITE, 0, (long) offsets.length * Long.BYTES, Arena.ofConfined());

            long indexFileSize = Files.size(basePath.resolve(INDEX_FILE)) + Long.BYTES;
            MemorySegment writeSegmentIndex = fcIndex.map(READ_WRITE, 0, indexFileSize, Arena.ofConfined());

            writeSegmentIndex.set(ValueLayout.JAVA_LONG, indexFileSize - Long.BYTES, instantNow);

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                offsets[index] = offsetData;
                MemorySegment.copy(key, 0, writeSegmentData, offsetData, key.byteSize());
                offsetData += key.byteSize();

                offsets[index + 1] = offsetData;
                if (value != null) {
                    MemorySegment.copy(value, 0, writeSegmentData, offsetData, value.byteSize());
                    offsetData += value.byteSize();
                }

                index += 2;
            }

            MemorySegment.copy(
                    MemorySegment.ofArray(offsets), ValueLayout.JAVA_LONG, 0,
                    writeSegmentOffset, ValueLayout.JAVA_LONG,0, offsets.length
            );
        }
    }

    public Entry<MemorySegment> readData(MemorySegment key) {
        if (listIndex == null || key == null) {
            return null;
        }

        for (int i = listIndex.size() - 1; i >= 0; i--) {
            MemorySegment offsetSegment = files.get(i).key();
            MemorySegment dataSegment = files.get(i).value();

            long offsetResult = binarySearch(key, offsetSegment, dataSegment);

            if (offsetResult != -1) {
                return Utils.getEntryByKeyOffset(offsetResult, offsetSegment, dataSegment);
            }
        }

        return null;
    }

    public List<PeekIterator> readDataFromTo(MemorySegment from, MemorySegment to) {
        if (listIndex == null) {
            return null;
        }

        boolean start = false;
        boolean end = false;

        long startKeyOffset;
        long endKeyOffset;

        List<PeekIterator> iterator = new ArrayList<>();

        for (int i = listIndex.size() - 1; i >= 0; i--) {
            MemorySegment offsetSegment = files.get(i).key();
            MemorySegment dataSegment = files.get(i).value();

            if (from != null) {
                startKeyOffset = binarySearch(from, offsetSegment, dataSegment);
            } else {
                startKeyOffset = 0;
                start = true;
            }

            if (to != null) {
                endKeyOffset = binarySearch(to, offsetSegment, dataSegment);
            } else {
                endKeyOffset = files.get(i).key().byteSize() - Long.BYTES * 2;
            }

            if (startKeyOffset == -1) {
                startKeyOffset = 0;
            } else {
                start = true;
            }

            if (endKeyOffset == -1) {
                endKeyOffset = files.get(i).key().byteSize() - Long.BYTES * 2;
            } else {
                end = true;
            }

            iterator.add(new PeekIterator(new FileIterator(offsetSegment, dataSegment, startKeyOffset, endKeyOffset), i));

            if (start && end) {
                break;
            }
        }

        return iterator;
    }
    private long binarySearch(MemorySegment key, MemorySegment offsetSegment, MemorySegment dataSegment) {
        long left = 0;
        long middle;
        long right = offsetSegment.byteSize() / Long.BYTES - 1;
        long keyOffset;
        long keySize;

        while (left <= right) {

            middle = (right - left) / 2 + left;

            long offset = middle * Long.BYTES * 2;
            if (offset >= offsetSegment.byteSize()) {
                return -1;
            }

            keyOffset = offsetSegment.get(ValueLayout.JAVA_LONG, offset);

            offset = middle * Long.BYTES * 2 + Long.BYTES;
            keySize = offsetSegment.get(ValueLayout.JAVA_LONG, offset) - keyOffset;

            MemorySegment keySegment = dataSegment.asSlice(keyOffset, keySize);

            int result = comparator.compare(keySegment, key);

            if (result < 0) {
                left = middle + 1;
            } else if (result > 0) {
                right = middle - 1;
            } else {
                return middle * Long.BYTES * 2;
            }
        }

        return -1;
    }

    public boolean isNullIndexList() {
        return listIndex == null;
    }

    public long getIndexListSize() {
        return listIndex.size();
    }

    private List<Long> getAllIndex() throws IOException {
        Path indexPath = basePath.resolve(INDEX_FILE);
        List<Long> index = new ArrayList<>();

        if (Files.notExists(indexPath)) {
            return null;
        }

        try (FileChannel fcIndex = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            MemorySegment indexMemorySegment = fcIndex.map(READ_ONLY, 0, Files.size(indexPath), Arena.ofConfined());

            int countIndex = (int) (Files.size(indexPath) / Long.BYTES);
            for (int i = 0; i < countIndex; i++) {
                index.add(indexMemorySegment.get(ValueLayout.JAVA_LONG, (long) i * Long.BYTES));
            }
        }

        return index;
    }
}
