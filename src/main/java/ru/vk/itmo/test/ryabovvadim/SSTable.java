package ru.vk.itmo.test.ryabovvadim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.ArrayList;
import java.nio.file.Path;
import java.util.Collection;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static ru.vk.itmo.test.ryabovvadim.FileUtils.DATA_FILE_EXT;
import static ru.vk.itmo.test.ryabovvadim.FileUtils.MEMORY_SEGMENT_COMPARATOR;
import static ru.vk.itmo.test.ryabovvadim.FileUtils.OFFSETS_FILE_EXT;

public class SSTable {

    private final String name;
    private final MemorySegment data;
    private final long[] offsets;
    private final int countRecords;

    public SSTable(Path prefix, String name, Arena arena) throws IOException {
        this.name = name;

        Path dataFile = FileUtils.makePath(prefix, name, DATA_FILE_EXT);
        Path offsetsFile = FileUtils.makePath(prefix, name, OFFSETS_FILE_EXT);

        try (FileChannel dataFileChannel = FileChannel.open(dataFile, READ)) {
            try (FileChannel offsetsFileChannel = FileChannel.open(offsetsFile, READ)) {
                this.data = dataFileChannel.map(READ_ONLY, 0, dataFileChannel.size(), arena);

                int countRecords = (int) (offsetsFileChannel.size() / JAVA_LONG.byteSize());
                this.offsets = new long[countRecords];
                MemorySegment offsetsSegment = offsetsFileChannel.map(READ_ONLY, 0, offsetsFileChannel.size(), arena);

                for (int i = 0; i < countRecords; ++i) {
                    offsets[i] = offsetsSegment.get(JAVA_LONG, i * JAVA_LONG.byteSize());
                }

                this.countRecords = offsets.length;
            }
        }
    }

    public Entry<MemorySegment> findEntry(MemorySegment key) {
        int offsetIndex = binSearchIndex(key, true);
        
        if (offsetIndex < 0) {
            return null;
        }
        return new BaseEntry<MemorySegment>(key, readValue(offsets[offsetIndex]));
    }
    
    public List<Entry<MemorySegment>> findEntries(MemorySegment from, MemorySegment to) {
        int fromIndex = 0;
        int toIndex = countRecords;
        
        if (from != null) {
            int fromOffsetIndex = binSearchIndex(from, true);
            fromIndex = fromOffsetIndex < 0 ? -(fromOffsetIndex + 1) : fromOffsetIndex;
        }
        if (to != null) {
            int toOffsetIndex = binSearchIndex(to, false);
            toIndex = toOffsetIndex < 0 ? -(toOffsetIndex + 1) + 1 : toOffsetIndex + 1;
        }
        
        List<Entry<MemorySegment>> entries = new ArrayList<>();
        for (int i = fromIndex; i < toIndex; ++i) {
            entries.add(new BaseEntry<MemorySegment>(
                readKey(offsets[i]),
                readValue(offsets[i])
            ));    
        }
        
        return entries;
    }

    public String getName() {
        return name;
    }
    
    private int binSearchIndex(MemorySegment key, boolean lowerBound) {
        int l = -1;
        int r = countRecords;

        while (l + 1 < r) {
            int mid = (l + r) / 2;
            MemorySegment curKey = readKey(offsets[mid]);
            int compareResult = MEMORY_SEGMENT_COMPARATOR.compare(curKey, key);

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

    private MemorySegment readKey(long offset) {
        byte sizeInfo = data.get(JAVA_BYTE, offset);
        offset += 1;
        int keySizeSize = sizeInfo >> 4;
        int valueSizeSize = sizeInfo & 0xf;

        byte[] keySizeInBytes = new byte[keySizeSize];
        for (int i = 0; i < keySizeSize; ++i) {
            keySizeInBytes[i] = data.get(JAVA_BYTE, offset);
            offset += 1;
        }
        long keySize = NumberUtils.fromBytes(keySizeInBytes);
        offset += valueSizeSize;

        return data.asSlice(offset, keySize);
    }

    private MemorySegment readValue(long offset) {
        byte sizeInfo = data.get(JAVA_BYTE, offset);
        offset += 1;
        int keySizeSize = sizeInfo >> 4;
        int valueSizeSize = sizeInfo & 0xf;

        byte[] keySizeInBytes = new byte[keySizeSize];
        byte[] valueSizeInBytes = new byte[valueSizeSize];
        for (int i = 0; i < keySizeSize; ++i) {
            keySizeInBytes[i] = data.get(JAVA_BYTE, offset);
            offset += 1;
        }
        for (int i = 0; i < valueSizeSize; ++i) {
            valueSizeInBytes[i] = data.get(JAVA_BYTE, offset);
            offset += 1;
        }
        long keySize = NumberUtils.fromBytes(keySizeInBytes);
        long valueSize = NumberUtils.fromBytes(valueSizeInBytes);

        return data.asSlice(offset + keySize, valueSize);
    }

    public static void save(Path prefix, String name, Collection<Entry<MemorySegment>> entries, Arena arena) throws IOException {
        Path dataFile = FileUtils.makePath(prefix, name, DATA_FILE_EXT);
        Path offsetsFile = FileUtils.makePath(prefix, name, OFFSETS_FILE_EXT);

        try (FileChannel dataFileChannel = FileChannel.open(dataFile, CREATE, WRITE, READ)) {
            try (FileChannel offsetsFileChannel = FileChannel.open(offsetsFile, CREATE, WRITE, READ)) {
                long dataSize = 0;
                for (Entry<MemorySegment> entry : entries) {
                    MemorySegment key = entry.key();
                    MemorySegment value = entry.value();
                    dataSize += 1;
                    dataSize += NumberUtils.toBytes(key.byteSize()).length +
                            NumberUtils.toBytes(value.byteSize()).length;
                    dataSize += key.byteSize() + value.byteSize();
                }

                MemorySegment dataSegment = dataFileChannel.map(READ_WRITE, 0, dataSize, arena);
                MemorySegment offsetsSegment = offsetsFileChannel.map(READ_WRITE, 0, JAVA_LONG.byteSize() * entries.size(), arena);
                long dataSegmentOffset = 0;
                long offsetsSegmentOffset = 0;

                for (Entry<MemorySegment> entry : entries) {
                    MemorySegment key = entry.key();
                    MemorySegment value = entry.value();
                    byte[] keySizeInBytes = NumberUtils.toBytes(key.byteSize());
                    byte[] valueSizeInBytes = NumberUtils.toBytes(value.byteSize());
                    byte sizeInfo = (byte) ((keySizeInBytes.length << 4) | valueSizeInBytes.length);

                    offsetsSegment.set(JAVA_LONG, offsetsSegmentOffset, dataSegmentOffset);
                    offsetsSegmentOffset += JAVA_LONG.byteSize();

                    dataSegment.set(JAVA_BYTE, dataSegmentOffset, sizeInfo);
                    dataSegmentOffset += 1;
                    MemorySegment.copy(
                            keySizeInBytes,
                            0,
                            dataSegment,
                            JAVA_BYTE,
                            dataSegmentOffset,
                            keySizeInBytes.length
                    );
                    dataSegmentOffset += keySizeInBytes.length;
                    MemorySegment.copy(
                            valueSizeInBytes,
                            0,
                            dataSegment,
                            JAVA_BYTE,
                            dataSegmentOffset,
                            valueSizeInBytes.length
                    );
                    dataSegmentOffset += valueSizeInBytes.length;
                    MemorySegment.copy(key, 0, dataSegment, dataSegmentOffset, key.byteSize());
                    dataSegmentOffset += key.byteSize();
                    MemorySegment.copy(value, 0, dataSegment, dataSegmentOffset, value.byteSize());
                    dataSegmentOffset += value.byteSize();
                }
            }
        }
    }
}
