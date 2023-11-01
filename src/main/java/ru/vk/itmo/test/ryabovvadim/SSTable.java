package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.iterators.FutureIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.LazyIterator;
import ru.vk.itmo.test.ryabovvadim.utils.FileUtils;
import ru.vk.itmo.test.ryabovvadim.utils.MemorySegmentUtils;
import ru.vk.itmo.test.ryabovvadim.utils.NumberUtils;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class SSTable {
    private final Path parentPath;
    private final long id;
    private final MemorySegment data;
    private final int countRecords;
    private final List<Long> offsets;

    public SSTable(Path parentPath, long id, Arena arena) throws IOException {
        this.parentPath = parentPath;
        this.id = id;
        Path dataFile = getDataFilePath();
        Path offsetsFile = getOffsetFilePath();

        try (FileChannel dataFileChannel = FileChannel.open(dataFile, READ)) {
            try (FileChannel offsetsFileChannel = FileChannel.open(offsetsFile, READ)) {
                this.data = dataFileChannel.map(MapMode.READ_ONLY, 0, dataFileChannel.size(), arena);
                this.countRecords = (int) (offsetsFileChannel.size() / Long.BYTES);
                this.offsets = new ArrayList<>();

                MemorySegment offsetsSegment = offsetsFileChannel.map(
                        MapMode.READ_ONLY, 0, offsetsFileChannel.size(), arena
                );
                for (int i = 0; i < countRecords; ++i) {
                    offsets.add(offsetsSegment.get(ValueLayout.JAVA_LONG, i * Long.BYTES));
                }
            }
        }
    }

    public Entry<MemorySegment> findEntry(MemorySegment key) {
        int offsetIndex = binSearchIndex(key, true);
        if (offsetIndex < 0) {
            return null;
        }
        return new BaseEntry<>(key, readValue(getRecordInfo(offsets.get(offsetIndex))));
    }

    public FutureIterator<Entry<MemorySegment>> findEntries(MemorySegment from, MemorySegment to) {
        int fromIndex = 0;
        int toIndex = countRecords;
        if (from != null) {
            int fromOffsetIndex = binSearchIndex(from, true);
            fromIndex = fromOffsetIndex < 0 ? -(fromOffsetIndex + 1) : fromOffsetIndex;
        }
        if (to != null) {
            int toOffsetIndex = binSearchIndex(to, false);
            toIndex = toOffsetIndex < 0 ? -toOffsetIndex : toOffsetIndex;
        }

        Iterator<Long> offsetsIterator = offsets.subList(fromIndex, toIndex).iterator();
        return new LazyIterator<>(
                () -> {
                    RecordInfo recordInfo = getRecordInfo(offsetsIterator.next());
                    return new BaseEntry<>(readKey(recordInfo), readValue(recordInfo));
                },
                offsetsIterator::hasNext
        );
    }

    private int binSearchIndex(MemorySegment key, boolean lowerBound) {
        int l = -1;
        int r = countRecords;
        while (l + 1 < r) {
            int mid = (l + r) / 2;
            RecordInfo recordInfo = getRecordInfo(offsets.get(mid));
            int compareResult = MemorySegmentUtils.compareMemorySegments(
                    data, recordInfo.keyOffset(), recordInfo.valueOffset(),
                    key, 0, key.byteSize()
            );

            if (compareResult == 0) {
                return mid;
            } else if (compareResult > 0) {
                r = mid;
            } else {
                l = mid;
            }
        }
        return lowerBound ? -r - 1 : -l - 1;
    }

    private RecordInfo getRecordInfo(long recordOffset) {
        long curOffset = recordOffset + 1;
        byte sizeInfo = data.get(ValueLayout.JAVA_BYTE, curOffset++);
        int keySizeSize = sizeInfo >> 4;
        int valueSizeSize = sizeInfo & 0xf;

        byte[] keySizeInBytes = new byte[keySizeSize];
        for (int i = 0; i < keySizeSize; ++i) {
            keySizeInBytes[i] = data.get(ValueLayout.JAVA_BYTE, curOffset++);
        }
        byte[] valueSizeInBytes = new byte[valueSizeSize];
        for (int i = 0; i < valueSizeSize; ++i) {
            valueSizeInBytes[i] = data.get(ValueLayout.JAVA_BYTE, curOffset++);
        }

        long keySize = NumberUtils.fromBytes(keySizeInBytes);
        long valueSize = NumberUtils.fromBytes(valueSizeInBytes);
        byte meta = data.get(ValueLayout.JAVA_BYTE, recordOffset);
        return new RecordInfo(meta, keySize, curOffset, valueSize, curOffset + keySize);
    }

    private MemorySegment readKey(RecordInfo recordInfo) {
        return data.asSlice(recordInfo.keyOffset(), recordInfo.keySize());
    }

    private MemorySegment readValue(RecordInfo recordInfo) {
        if (SSTableMeta.isRemovedValue(recordInfo.meta())) {
            return null;
        }
        return data.asSlice(recordInfo.valueOffset(), recordInfo.valueSize());
    }

    public static void save(
            Path prefix,
            long id,
            Collection<Entry<MemorySegment>> entries,
            Arena arena
    ) throws IOException {
        Path dataFile = FileUtils.makePath(prefix, Long.toString(id), FileUtils.DATA_FILE_EXT);
        Path offsetsFile = FileUtils.makePath(prefix, Long.toString(id), FileUtils.OFFSETS_FILE_EXT);
        try (FileChannel dataFileChannel = FileChannel.open(dataFile, CREATE, WRITE, READ)) {
            try (FileChannel offsetsFileChannel = FileChannel.open(offsetsFile, CREATE, WRITE, READ)) {
                long dataSize = 0;
                for (Entry<MemorySegment> entry : entries) {
                    dataSize += 2;
                    MemorySegment key = entry.key();
                    MemorySegment value = entry.value();

                    dataSize += NumberUtils.toBytes(key.byteSize()).length + key.byteSize();
                    if (value != null) {
                        dataSize += NumberUtils.toBytes(value.byteSize()).length + value.byteSize();
                    }
                }

                MemorySegment dataSegment = dataFileChannel.map(MapMode.READ_WRITE, 0, dataSize, arena);
                MemorySegment offsetsSegment = offsetsFileChannel.map(
                        MapMode.READ_WRITE,
                        0,
                        Long.BYTES * entries.size(),
                        arena
                );
                long dataSegmentOffset = 0;
                long offsetsSegmentOffset = 0;

                for (Entry<MemorySegment> entry : entries) {
                    MemorySegment key = entry.key();
                    MemorySegment value = entry.value();
                    byte[] keySizeInBytes = NumberUtils.toBytes(key.byteSize());
                    byte[] valueSizeInBytes = value == null
                            ? new byte[0]
                            : NumberUtils.toBytes(value.byteSize());

                    offsetsSegment.set(ValueLayout.JAVA_LONG, offsetsSegmentOffset, dataSegmentOffset);
                    offsetsSegmentOffset += Long.BYTES;

                    byte meta = SSTableMeta.buildMeta(entry);
                    byte sizeInfo = (byte) ((keySizeInBytes.length << 4) | valueSizeInBytes.length);
                    dataSegment.set(ValueLayout.JAVA_BYTE, dataSegmentOffset++, meta);
                    dataSegment.set(ValueLayout.JAVA_BYTE, dataSegmentOffset++, sizeInfo);

                    MemorySegmentUtils.copyByteArray(keySizeInBytes, dataSegment, dataSegmentOffset);
                    dataSegmentOffset += keySizeInBytes.length;
                    MemorySegmentUtils.copyByteArray(valueSizeInBytes, dataSegment, dataSegmentOffset);
                    dataSegmentOffset += valueSizeInBytes.length;
                    MemorySegment.copy(key, 0, dataSegment, dataSegmentOffset, key.byteSize());
                    dataSegmentOffset += key.byteSize();
                    if (value != null) {
                        MemorySegment.copy(
                                value, 0, dataSegment, dataSegmentOffset, value.byteSize()
                        );
                        dataSegmentOffset += value.byteSize();
                    }
                }
            }
        }
    }

    public void delete() throws IOException {
        Files.deleteIfExists(getDataFilePath());
        Files.deleteIfExists(getOffsetFilePath());
    }

    private Path getDataFilePath() {
        return FileUtils.makePath(parentPath, Long.toString(id), FileUtils.DATA_FILE_EXT);
    }

    private Path getOffsetFilePath() {
        return FileUtils.makePath(parentPath, Long.toString(id), FileUtils.OFFSETS_FILE_EXT);
    }

    public long getId() {
        return id;
    }

    private static final class SSTableMeta {
        private static final byte REMOVE_VALUE = 0x1;

        public static boolean isRemovedValue(byte meta) {
            return (meta & REMOVE_VALUE) == REMOVE_VALUE;
        }

        public static byte buildMeta(Entry<MemorySegment> entry) {
            byte meta = 0;

            if (entry.value() == null) {
                meta |= SSTableMeta.REMOVE_VALUE;
            }
            return meta;
        }

        private SSTableMeta() {
        }
    }

    private static final class RecordInfo {
        private final byte meta;
        private final long keySize;
        private final long keyOffset;
        private final long valueSize;
        private final long valueOffset;

        private RecordInfo(
                byte meta,
                long keySize,
                long keyOffset,
                long valueSize,
                long valueOffset
        ) {
            this.meta = meta;
            this.keySize = keySize;
            this.keyOffset = keyOffset;
            this.valueSize = valueSize;
            this.valueOffset = valueOffset;
        }

        public byte meta() {
            return meta;
        }

        public long keySize() {
            return keySize;
        }

        public long keyOffset() {
            return keyOffset;
        }

        public long valueSize() {
            return valueSize;
        }

        public long valueOffset() {
            return valueOffset;
        }
    }
}
