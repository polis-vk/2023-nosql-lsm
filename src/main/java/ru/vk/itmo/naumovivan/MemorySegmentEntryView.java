package ru.vk.itmo.naumovivan;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static ru.vk.itmo.naumovivan.DiskStorageUtils.endOfValue;
import static ru.vk.itmo.naumovivan.DiskStorageUtils.normalize;
import static ru.vk.itmo.naumovivan.DiskStorageUtils.recordsCount;
import static ru.vk.itmo.naumovivan.DiskStorageUtils.startOfKey;
import static ru.vk.itmo.naumovivan.DiskStorageUtils.startOfValue;

public class MemorySegmentEntryView implements Comparable<MemorySegmentEntryView> {
    private final MemorySegment keyPage;
    private final long keyOffsetStart;
    private final long keyOffsetEnd;
    private final MemorySegment valuePage;
    private final long valueOffsetStart;
    private final long valueOffsetEnd;

    public static MemorySegmentEntryView fromEntry(final Entry<MemorySegment> entry) {
        final MemorySegment key = entry.key();
        final MemorySegment value = entry.value();
        return new MemorySegmentEntryView(key, 0, key.byteSize(),
                value, 0, value == null ? -1 : value.byteSize());
    }

    public MemorySegmentEntryView(final MemorySegment keyPage,
                                  final long keyOffsetStart,
                                  final long keyOffsetEnd,
                                  final MemorySegment valuePage,
                                  final long valueOffsetStart,
                                  final long valueOffsetEnd) {
        this.keyPage = keyPage;
        this.keyOffsetStart = keyOffsetStart;
        this.keyOffsetEnd = keyOffsetEnd;
        this.valuePage = valuePage;
        this.valueOffsetStart = valueOffsetStart;
        this.valueOffsetEnd = valueOffsetEnd;
    }

    public long keySize() {
        return keyOffsetEnd - keyOffsetStart;
    }

    public long valueSize() {
        return isValueDeleted() ? 0 : valueOffsetEnd - valueOffsetStart;
    }

    public boolean isValueDeleted() {
        return valuePage == null || (valueOffsetStart & 1L << 63) != 0;
    }

    public void copyKey(final MemorySegment filePage, final long fileOffset) {
        MemorySegment.copy(keyPage, keyOffsetStart, filePage, fileOffset, keySize());
    }

    public void copyValue(final MemorySegment filePage, final long fileOffset) {
        MemorySegment.copy(valuePage, valueOffsetStart, filePage, fileOffset, valueSize());
    }

    @Override
    public int compareTo(final MemorySegmentEntryView other) {
        final long mismatch = MemorySegment.mismatch(keyPage, keyOffsetStart, keyOffsetEnd,
                other.keyPage, other.keyOffsetStart, other.keyOffsetEnd);
        if (mismatch == -1) {
            return 0;
        }

        if (mismatch == keyOffsetEnd - keyOffsetStart) {
            return -1;
        }
        if (mismatch == other.keyOffsetEnd - other.keyOffsetStart) {
            return 1;
        }

        final byte b1 = keyPage.get(ValueLayout.JAVA_BYTE, keyOffsetStart + mismatch);
        final byte b2 = other.keyPage.get(ValueLayout.JAVA_BYTE, other.keyOffsetStart + mismatch);
        return Byte.compare(b1, b2);
    }

    public static Iterator<MemorySegmentEntryView> makeIterator(final MemorySegment memorySegment) {
        final long recordsCount = recordsCount(memorySegment);

        return new Iterator<>() {
            long index;

            @Override
            public boolean hasNext() {
                return index < recordsCount;
            }

            @Override
            public MemorySegmentEntryView next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                final long startOfKey = startOfKey(memorySegment, index);
                final long startOfValue = startOfValue(memorySegment, index);
                final long endOfKey = normalize(startOfValue);
                final long endOfValue = endOfValue(memorySegment, index, recordsCount);
                index++;
                return new MemorySegmentEntryView(memorySegment, startOfKey, endOfKey,
                        memorySegment, startOfValue, endOfValue);
            }
        };
    }
}
