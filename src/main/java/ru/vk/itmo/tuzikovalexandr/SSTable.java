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
    private final MemorySegment readSegmentIndex;
    private static final String OFFSET_PATH = "offset";
    private static final String FILE_PATH = "data";
    //private final long[] offsets;
    private MemorySegmentComparator comparator;

    public SSTable(Config config) throws IOException {
        this.filePath = config.basePath().resolve(FILE_PATH);
        this.offsetPath = config.basePath().resolve(OFFSET_PATH);

        //offsets = new long[2];

        readArena = Arena.ofConfined();

        if (Files.notExists(filePath)) {
            readSegmentData = null;
            readSegmentIndex = null;
            return;
        }

        try (FileChannel fcData = FileChannel.open(filePath, StandardOpenOption.READ);
             FileChannel fcIndex = FileChannel.open(filePath, StandardOpenOption.READ)) {
            readSegmentData = fcData.map(READ_ONLY, 0, Files.size(filePath), readArena);
            readSegmentIndex = fcData.map(READ_ONLY, 0, Files.size(filePath), readArena);
        }
    }

    public void saveMemData(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (!readArena.scope().isAlive()) {
            return;
        }

        readArena.close();

        long offsetData = 0;
        long offsetIndex = 0;
        long memorySize = entries.stream().mapToLong(
                entry -> entry.key().byteSize() + entry.value().byteSize()
        ).sum();

        try (FileChannel fcData = FileChannel.open(filePath, openOptions);
             FileChannel fcIndex = FileChannel.open(offsetPath, openOptions)) {

            MemorySegment writeSegmentData = fcData.map(READ_WRITE, 0, memorySize, Arena.ofConfined());
            MemorySegment writeSegmentIndex = fcIndex.map(READ_WRITE, 0, memorySize, Arena.ofConfined());

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();
                MemorySegment offset;

//                writeSegmentData.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, key.byteSize());
//                offset += Long.BYTES;
//                writeSegmentData.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.byteSize());
//                offset += Long.BYTES;

                offset = Utils.longToMemorySegment(offsetData);
                MemorySegment.copy(offset, 0, writeSegmentIndex, offsetIndex, offset.byteSize());

                MemorySegment.copy(key, 0, writeSegmentData, offsetData, key.byteSize());
                offsetData += key.byteSize();

                offset = Utils.longToMemorySegment(offsetData);
                MemorySegment.copy(offset, 0, writeSegmentIndex, offsetIndex, offset.byteSize());

                MemorySegment.copy(value, 0, writeSegmentData, offsetData, value.byteSize());
                offsetData += value.byteSize();

//                writeSegmentData.
//                        writeSegmentData.asSlice(offset).copyFrom(entry.key());
//                offset += entry.key().byteSize();
//
//                writeSegmentData.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
//                offset += Long.BYTES;
//                writeSegmentData.asSlice(offset).copyFrom(entry.value());
//                offset += entry.value().byteSize();
            }
        }
    }

    public Entry<MemorySegment> readData(MemorySegment key) {
        if (readSegmentData == null) {
            return null;
        }

        return binarySearch(key);

//        long offset = 0L;
//
//        while (offset < readSegmentData.byteSize()) {
//            long keySize = readSegmentData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
//            offset += Long.BYTES;
//
//            long valueSize = readSegmentData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + keySize);
//
//            if (keySize != key.byteSize()) {
//                offset += keySize + valueSize + Long.BYTES;
//                continue;
//            }
//
//            MemorySegment keySegment = readSegmentData.asSlice(offset, keySize);
//            offset += keySize + Long.BYTES;
//
//            if (key.mismatch(keySegment) == -1) {
//                MemorySegment valueSegment = readSegmentData.asSlice(offset, valueSize);
//                return new BaseEntry<>(keySegment, valueSegment);
//            }
//
//            offset += valueSize;
//        }
//
//        return null;
    }

    private Entry<MemorySegment> binarySearch(MemorySegment key) {
        int left = 0;
        int middle;
        int right = (int) readSegmentIndex.byteSize();
        long keyOffset;
        long keySize;
        long valueOffset;
        long valueSize;

        while (left <= right) {
            middle = (right - left) / 2 + left;

            keyOffset = readSegmentIndex.asSlice(middle, ValueLayout.JAVA_LONG).get(ValueLayout.JAVA_LONG, 0);
            keySize = readSegmentIndex.asSlice(middle + Long.BYTES, ValueLayout.JAVA_LONG)
                    .get(ValueLayout.JAVA_LONG, 0) - keyOffset;

            MemorySegment keySegment = getSegmentByOffset(keyOffset, keySize);

            int result = comparator.compare(keySegment, key);

            if (result < 0) {
                left = middle + 1;
            } else if (result > 0) {
                right = middle - 1;
            } else {
                valueOffset = readSegmentIndex.asSlice(middle + 8, 8).get(ValueLayout.JAVA_LONG, 0);
                valueSize = readSegmentIndex.asSlice(middle + 16, 8)
                        .get(ValueLayout.JAVA_LONG, 0) - valueOffset;

                MemorySegment valueSegment = getSegmentByOffset(valueOffset, valueSize);

                return new BaseEntry<>(keySegment, valueSegment);
            }
        }

        return null;
    }

    private MemorySegment getSegmentByOffset(long offset, long segmentSize) {
        return readSegmentData.asSlice(offset, segmentSize);
    }
}
