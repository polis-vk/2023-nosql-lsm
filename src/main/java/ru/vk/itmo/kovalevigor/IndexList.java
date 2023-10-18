package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.*;

public class IndexList extends AbstractList<Entry<MemorySegment>> implements RandomAccess {

    public static final long INDEX_ENTRY_SIZE = 2 * ValueLayout.JAVA_LONG.byteSize();
    public static final long META_INFO_SIZE = 2 * ValueLayout.JAVA_LONG.byteSize();
    public static long MAX_BYTE_SIZE = META_INFO_SIZE + Integer.MAX_VALUE * INDEX_ENTRY_SIZE;

    private final MemorySegment indexSegment;
    private final MemorySegment dataSegment;
    private final long valuesOffset;

    public IndexList(MemorySegment indexSegment, final MemorySegment dataSegment) {
        if (indexSegment.byteSize() > MAX_BYTE_SIZE) {
            indexSegment = indexSegment.asSlice(0, MAX_BYTE_SIZE);
        }
        this.indexSegment = indexSegment;
        this.dataSegment = dataSegment;

        this.valuesOffset = readOffset(ValueLayout.JAVA_LONG.byteSize());
    }

    private long getEntryOffset(final int index) {
        if (size() <= index) {
            return -1;
        }
        return META_INFO_SIZE + INDEX_ENTRY_SIZE * index;
    }

    private long readOffset(final long offset) {
        return indexSegment.get(ValueLayout.JAVA_LONG, offset);
    }

    @Override
    public Entry<MemorySegment> get(final int index) {
        final long offset = getEntryOffset(index);

        final long keyOffset = readOffset(offset);
        final long valueOffset = readOffset(offset + ValueLayout.JAVA_LONG.byteSize());
        final long nextEntryOffset = getEntryOffset(index + 1);

        long keySize = valuesOffset - keyOffset;
        if (nextEntryOffset != -1) {
            keySize = readOffset(nextEntryOffset) - keyOffset;
        }
        final MemorySegment value;
        if (valueOffset != -1) {
            long valueSize = dataSegment.byteSize() - valueOffset;
            if (nextEntryOffset != -1) {
                valueSize = readOffset(nextEntryOffset + ValueLayout.JAVA_LONG.byteSize()) - valueOffset;
            }
            value = dataSegment.asSlice(valueOffset, valueSize);
        } else {
            value = null;
        }
        final MemorySegment key = dataSegment.asSlice(keyOffset, keySize);

        return new BaseEntry<>(key, value);
    }

    @Override
    public int size() {
        return (int)((indexSegment.byteSize() - META_INFO_SIZE) / INDEX_ENTRY_SIZE);
    }

    public static long getFileSize(final SortedMap<MemorySegment, Entry<MemorySegment>> map) {
        return META_INFO_SIZE + map.size() * INDEX_ENTRY_SIZE;
    }

    public static void write(
            final MemorySegment writer,
            final long[][] offsets,
            final long fileSize
    ) {

        long valuesOffset = fileSize;
        for (final long[] entry: offsets) {
            if (entry[1] != -1) {
                valuesOffset = entry[1];
                break;
            }
        }

        writer.set(ValueLayout.JAVA_LONG, 0, META_INFO_SIZE);
        writer.set(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG.byteSize(), valuesOffset);
        long offset = META_INFO_SIZE;
        for (final long[] entry: offsets) {
            writer.set(ValueLayout.JAVA_LONG, offset, entry[0]);
            writer.set(ValueLayout.JAVA_LONG, offset += ValueLayout.JAVA_LONG.byteSize(), entry[1]);
            offset += ValueLayout.JAVA_LONG.byteSize();
        }
    }

}
