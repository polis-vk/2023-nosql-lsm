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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class SSTable {
    private final String name;
    private final MemorySegment data;
    private final int countRecords;
    private final List<Long> offsets;

    public SSTable(Path prefix, String name, Arena arena) throws IOException {
        this.name = name;

        Path dataFile = FileUtils.makePath(prefix, name, FileUtils.DATA_FILE_EXT);
        Path offsetsFile = FileUtils.makePath(prefix, name, FileUtils.OFFSETS_FILE_EXT);

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

    public String getName() {
        return name;
    }

    private int binSearchIndex(MemorySegment key, boolean lowerBound) {
        int l = -1;
        int r = countRecords;

        while (l + 1 < r) {
            int mid = (l + r) / 2;
            RecordInfo recordInfo = getRecordInfo(offsets.get(mid));
            int compareResult = MemorySegmentUtils.compareMemorySegments(
                    data, recordInfo.getKeyOffset(), recordInfo.getValueOffset(),
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
        long curOffset = recordOffset;
        ++curOffset;

        byte sizeInfo = data.get(ValueLayout.JAVA_BYTE, curOffset);
        ++curOffset;
        int keySizeSize = sizeInfo >> 4;
        int valueSizeSize = sizeInfo & 0xf;

        byte[] keySizeInBytes = new byte[keySizeSize];
        for (int i = 0; i < keySizeSize; ++i) {
            keySizeInBytes[i] = data.get(ValueLayout.JAVA_BYTE, curOffset);
            ++curOffset;
        }
        byte[] valueSizeInBytes = new byte[valueSizeSize];
        for (int i = 0; i < valueSizeSize; ++i) {
            valueSizeInBytes[i] = data.get(ValueLayout.JAVA_BYTE, curOffset);
            ++curOffset;
        }

        long keySize = NumberUtils.fromBytes(keySizeInBytes);
        long valueSize = NumberUtils.fromBytes(valueSizeInBytes);
        byte meta = data.get(ValueLayout.JAVA_BYTE, recordOffset);

        return new RecordInfo(meta, keySize, curOffset, valueSize, curOffset + keySize);
    }

    private MemorySegment readKey(RecordInfo recordInfo) {
        return data.asSlice(recordInfo.getKeyOffset(), recordInfo.getKeySize());
    }

    private MemorySegment readValue(RecordInfo recordInfo) {
        if (SSTableMeta.isRemovedValue(recordInfo.getMeta())) {
            return null;
        }
        return data.asSlice(recordInfo.getValueOffset(), recordInfo.getValueSize());
    }

    public static void save(
            Path prefix,
            String name,
            Collection<Entry<MemorySegment>> entries,
            Arena arena
    ) throws IOException {
        Path dataFile = FileUtils.makePath(prefix, name, FileUtils.DATA_FILE_EXT);
        Path offsetsFile = FileUtils.makePath(prefix, name, FileUtils.OFFSETS_FILE_EXT);

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

                MemorySegment dataSegment = dataFileChannel.map(
                        MapMode.READ_WRITE, 0, dataSize, arena
                );
                MemorySegment offsetsSegment = offsetsFileChannel.map(
                        MapMode.READ_WRITE, 0, Long.BYTES * entries.size(), arena
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

                    byte meta = buildMeta(entry);
                    byte sizeInfo = (byte) ((keySizeInBytes.length << 4) | valueSizeInBytes.length);

                    dataSegment.set(ValueLayout.JAVA_BYTE, dataSegmentOffset, meta);
                    dataSegmentOffset += 1;
                    dataSegment.set(ValueLayout.JAVA_BYTE, dataSegmentOffset, sizeInfo);
                    dataSegmentOffset += 1;

                    MemorySegment.copy(
                            keySizeInBytes,
                            0,
                            dataSegment,
                            ValueLayout.JAVA_BYTE,
                            dataSegmentOffset,
                            keySizeInBytes.length
                    );
                    dataSegmentOffset += keySizeInBytes.length;
                    MemorySegment.copy(
                            valueSizeInBytes,
                            0,
                            dataSegment,
                            ValueLayout.JAVA_BYTE,
                            dataSegmentOffset,
                            valueSizeInBytes.length
                    );
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

    private static byte buildMeta(Entry<MemorySegment> entry) {
        byte meta = 0;

        if (entry.value() == null) {
            meta |= SSTableMeta.REMOVE_VALUE;
        }
        return meta;
    }

    private static class SSTableMeta {
        private static final byte REMOVE_VALUE = 0x1;

        private static boolean isRemovedValue(byte meta) {
            return (meta & REMOVE_VALUE) == REMOVE_VALUE;
        }
    }

    private static class RecordInfo {
        private final byte meta;
        private final long keySize;
        private final long keyOffset;
        private final long valueSize;
        private final long valueOffset;

        public RecordInfo(
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

        public byte getMeta() {
            return meta;
        }

        public long getKeySize() {
            return keySize;
        }

        public long getKeyOffset() {
            return keyOffset;
        }

        public long getValueSize() {
            return valueSize;
        }

        public long getValueOffset() {
            return valueOffset;
        }
    }
}
