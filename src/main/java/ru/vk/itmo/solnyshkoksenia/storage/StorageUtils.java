package ru.vk.itmo.solnyshkoksenia.storage;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;

public class StorageUtils {
    protected static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    protected static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    protected static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    protected static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    protected static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    protected static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    protected static long normalize(long value) {
        return value & ~(1L << 63);
    }

    protected static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    protected static MemorySegment mapFile(FileChannel fileChannel, long size, Arena arena) throws IOException {
        return fileChannel.map(
                FileChannel.MapMode.READ_WRITE,
                0,
                size,
                arena
        );
    }

    protected static Entry<Long> putEntry(MemorySegment fileSegment, Entry<Long> offsets, Entry<MemorySegment> entry) {
        long dataOffset = offsets.key();
        long indexOffset = offsets.value();
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
        indexOffset += Long.BYTES;

        MemorySegment key = entry.key();
        MemorySegment value = entry.value();
        MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
        dataOffset += key.byteSize();

        if (value == null) {
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
        } else {
            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
            MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
            dataOffset += value.byteSize();
        }
        indexOffset += Long.BYTES;

        return new BaseEntry<>(dataOffset, indexOffset);
    }

    private static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }
}
