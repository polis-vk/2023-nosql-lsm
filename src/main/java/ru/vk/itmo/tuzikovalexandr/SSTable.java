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
import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

public class SSTable {

    private static final Set<OpenOption> openOptions = Set.of(
            StandardOpenOption.CREATE, StandardOpenOption.READ,
            StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING
    );
    private final Path filePath;
    private final Path offsetPath;
    private final Arena readArena;
    private final MemorySegment readSegmentData;
    private final MemorySegment readSegmentOffset;
    private static final String OFFSET_PATH = "offset";
    private static final String FILE_PATH = "data";
    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();

    public SSTable(Config config) throws IOException {
        this.filePath = config.basePath().resolve(FILE_PATH);
        this.offsetPath = config.basePath().resolve(OFFSET_PATH);

        readArena = Arena.ofConfined();

        if (Files.notExists(filePath)) {
            readSegmentData = null;
            readSegmentOffset = null;
            return;
        }

        try (FileChannel fcData = FileChannel.open(filePath, StandardOpenOption.READ);
             FileChannel fcIndex = FileChannel.open(offsetPath, StandardOpenOption.READ)) {
            readSegmentData = fcData.map(READ_ONLY, 0, Files.size(filePath), readArena);
            readSegmentOffset = fcIndex.map(READ_ONLY, 0, Files.size(offsetPath), readArena);
        }
    }

    public void saveMemData(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (!readArena.scope().isAlive()) {
            return;
        }

        readArena.close();

        long[] offsets = new long[entries.size() * 2];

        long offsetData = 0;
        long memorySize = entries.stream().mapToLong(
                entry -> entry.key().byteSize() + entry.value().byteSize()
        ).sum();
        int index = 0;

        try (FileChannel fcData = FileChannel.open(filePath, openOptions);
             FileChannel fcIndex = FileChannel.open(offsetPath, openOptions)) {

            MemorySegment writeSegmentData = fcData.map(READ_WRITE, 0, memorySize, Arena.ofConfined());
            MemorySegment writeSegmentOffset = fcIndex.map(READ_WRITE, 0, (long) offsets.length * Long.BYTES, Arena.ofConfined());

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                offsets[index] = offsetData;
                MemorySegment.copy(key, 0, writeSegmentData, offsetData, key.byteSize());
                offsetData += key.byteSize();

                offsets[index + 1] = offsetData;
                MemorySegment.copy(value, 0, writeSegmentData, offsetData, value.byteSize());
                offsetData += value.byteSize();

                index += 2;
            }

            MemorySegment.copy(
                    MemorySegment.ofArray(offsets), ValueLayout.JAVA_LONG, 0,
                    writeSegmentOffset, ValueLayout.JAVA_LONG,0, offsets.length
            );
        }
    }

    public Entry<MemorySegment> readData(MemorySegment key) {
        if (readSegmentData == null) {
            return null;
        }

        return binarySearch(key);
    }

    private Entry<MemorySegment> binarySearch(MemorySegment key) {
        long left = 0;
        long middle;
        long right = readSegmentOffset.byteSize() / Long.BYTES - 1;
        long keyOffset;
        long keySize;
        long valueOffset;
        long valueSize;

        while (left <= right) {

            middle = (right - left) / 2 + left;

            long offset = middle * Long.BYTES * 2;
            if (offset >= readSegmentOffset.byteSize()) {
                return null;
            }

            keyOffset = readSegmentOffset.get(ValueLayout.JAVA_LONG, offset);

            offset = middle * Long.BYTES * 2 + Long.BYTES;
            keySize = readSegmentOffset.get(ValueLayout.JAVA_LONG, offset) - keyOffset;

            MemorySegment keySegment = getSegmentByOffsetAndSize(keyOffset, keySize);

            int result = comparator.compare(keySegment, key);

            if (result < 0) {
                left = middle + 1;
            } else if (result > 0) {
                right = middle - 1;
            } else {
                offset = middle * Long.BYTES * 2 + Long.BYTES;
                MemorySegment valueSegment;

                valueOffset = readSegmentOffset.get(ValueLayout.JAVA_LONG, offset);

                offset = (middle + 1) * Long.BYTES * 2;
                if (offset >= readSegmentOffset.byteSize()) {
                    valueSegment = getSegmentByOffset(valueOffset);

                } else {
                    valueSize = readSegmentOffset.get(ValueLayout.JAVA_LONG, offset) - valueOffset;
                    valueSegment = getSegmentByOffsetAndSize(valueOffset, valueSize);
                }

                return new BaseEntry<>(keySegment, valueSegment);
            }
        }

        return null;
    }

    private MemorySegment getSegmentByOffsetAndSize(long offset, long segmentSize) {
        return readSegmentData.asSlice(offset, segmentSize);
    }

    private MemorySegment getSegmentByOffset(long offset) {
        return readSegmentData.asSlice(offset);
    }
}
