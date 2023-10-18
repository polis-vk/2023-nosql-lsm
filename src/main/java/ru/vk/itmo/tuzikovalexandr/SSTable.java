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
import java.util.stream.Stream;

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
    private final Comparator<MemorySegment> comparator;
    private final List<BaseEntry<MemorySegment>> files;
    private final List<String> listIndex;

    public SSTable(Config config) throws IOException {
        this.basePath = config.basePath();

        if(Files.notExists(basePath)) {
            listIndex = null;
            readArena = null;
            files = null;
            comparator = null;
            return;
        }

        listIndex = getAllIndex();
        readArena = Arena.ofConfined();
        files = new ArrayList<>();
        comparator = MemorySegmentComparator::compare;

        for (String index : listIndex) {
            Path offsetFullPath = basePath.resolve(OFFSET_PREFIX + index);
            Path fileFullPath = basePath.resolve(FILE_PREFIX + index);

            try (FileChannel fcData = FileChannel.open(offsetFullPath, StandardOpenOption.READ);
                 FileChannel fcOffset = FileChannel.open(fileFullPath, StandardOpenOption.READ)) {
                MemorySegment readSegmentOffset = fcData.map(READ_ONLY, 0, Files.size(offsetFullPath), readArena);
                MemorySegment readSegmentData = fcOffset.map(READ_ONLY, 0, Files.size(fileFullPath), readArena);

                files.add(new BaseEntry<>(readSegmentOffset, readSegmentData));
            }
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
            } else {
                memorySize += Long.BYTES;
            }
        }
        int index = 0;

        long instantNow = Instant.now().toEpochMilli();

        try (FileChannel fcData = FileChannel.open(basePath.resolve(FILE_PREFIX + instantNow), openOptions);
             FileChannel fcOffset = FileChannel.open(basePath.resolve(OFFSET_PREFIX + instantNow), openOptions)) {

            MemorySegment writeSegmentData = fcData.map(READ_WRITE, 0, memorySize, Arena.ofConfined());
            MemorySegment writeSegmentOffset = fcOffset.map(READ_WRITE, 0, (long) offsets.length * Long.BYTES, Arena.ofConfined());

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
                } else {
                    writeSegmentData.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, -1L);
                    offsetData += Long.BYTES;
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

            if (offsetResult >= 0) {
                return Utils.getEntryByKeyOffset(offsetResult, offsetSegment, dataSegment);
            }
        }

        return null;
    }

    public List<PeekIterator> readDataFromTo(MemorySegment from, MemorySegment to) {
        if (listIndex == null) {
            return null;
        }

        List<PeekIterator> iterator = new ArrayList<>();

        for (int i = listIndex.size() - 1; i >= 0; i--) {
            MemorySegment offsetSegment = files.get(i).key();
            MemorySegment dataSegment = files.get(i).value();

            long start = from == null ? 0 : Math.abs(binarySearch(from, offsetSegment, dataSegment));
            long end = to == null ? offsetSegment.byteSize() - Long.BYTES * 2 :
                    Math.abs(binarySearch(to, offsetSegment, dataSegment)) - Long.BYTES * 2;

            if (start > end) {
                continue;
            }

            iterator.add(new PeekIterator(new FileIterator(offsetSegment, dataSegment, start, end), i));
        }

        return iterator;
    }
    private long binarySearch(MemorySegment key, MemorySegment offsetSegment, MemorySegment dataSegment) {
        long left = 0;
        long right = offsetSegment.byteSize() / Long.BYTES - 1;

        while (left <= right) {

            long middle = (right - left) / 2 + left;

            long offset = middle * Long.BYTES * 2;
            if (offset >= offsetSegment.byteSize()) {
                return -left * Long.BYTES * 2;
            }

            long keyOffset = offsetSegment.get(ValueLayout.JAVA_LONG, offset);

            offset = middle * Long.BYTES * 2 + Long.BYTES;
            long keySize = offsetSegment.get(ValueLayout.JAVA_LONG, offset) - keyOffset;

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

        return -left * Long.BYTES * 2;
    }

    public boolean isNullIndexList() { return listIndex == null; }

    public long getIndexListSize() {
        if (isNullIndexList()) {
            return 0;
        }
        return listIndex.size();
    }

    private List<String> getAllIndex() throws IOException {
        List<String> index = new ArrayList<>();

        try (Stream<Path> fileStream = Files.list(basePath)) {
            fileStream.forEach(path -> {
                String fileName = path.getFileName().toString();
                if (fileName.startsWith(FILE_PREFIX)) {
                    index.add(Utils.getIndexFromString(fileName));
                }
            });
        }

        return index;
    }
}
