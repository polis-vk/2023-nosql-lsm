package ru.vk.itmo.proninvalentin.utils;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class MemorySegmentUtils {
    private static final byte TOMBSTONE_VALUE = -1;

    private MemorySegmentUtils() {
    }

    public static MemorySegment getKeyBySizeOffset(MemorySegment readValuesMS, long sizeOffset) {
        long valueSize = readValuesMS.get(ValueLayout.JAVA_LONG_UNALIGNED, sizeOffset);
        long valueOffset = sizeOffset + Long.BYTES;
        return readValuesMS.asSlice(valueOffset, valueSize);
    }

    public static MemorySegment getValueBySizeOffset(MemorySegment readValuesMS, long sizeOffset) {
        long valueSize = readValuesMS.get(ValueLayout.JAVA_LONG_UNALIGNED, sizeOffset);
        if (valueSize == TOMBSTONE_VALUE) {
            return null;
        }
        long valueOffset = sizeOffset + Long.BYTES;
        return readValuesMS.asSlice(valueOffset, valueSize);
    }

    public static BaseEntry<MemorySegment> getEntryByIndex(MemorySegment readValuesMS,
                                                           MemorySegment readOffsetsMS,
                                                           long index) {
        long entryOffset = getEntryOffsetByIndex(readOffsetsMS, index);
        MemorySegment key = getKeyBySizeOffset(readValuesMS, entryOffset);
        long valueSizeOffset = entryOffset + Long.BYTES + key.byteSize();
        MemorySegment value = getValueBySizeOffset(readValuesMS, valueSizeOffset);
        if (value == null) {
            return new BaseEntry<>(MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)), null);
        } else {
            return new BaseEntry<>(
                    MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)),
                    MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)));
        }
    }

    public static long getEntryOffsetByIndex(MemorySegment readOffsetsMS,
                                             long index) {
        long entryOffset = index * Long.BYTES;
        return readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
    }

    // Возвращает индекс значения с указанным ключом, если он есть, иначе возвращает -1
    public static long binarySearch(MemorySegment readValuesMS,
                                    MemorySegment readOffsetsMS,
                                    MemorySegment desiredKey,
                                    Comparator<MemorySegment> comparator) {
        long valuesCount = readOffsetsMS.byteSize() / Long.BYTES;
        long l = 0;
        long r = valuesCount - 1;

        while (l <= r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Long.BYTES);
            MemorySegment key = getKeyBySizeOffset(readValuesMS, keySizeOffset);

            if (comparator.compare(key, desiredKey) == 0) {
                return m;
            } else if (comparator.compare(key, desiredKey) < 0) {
                l = m + 1;
            } else {
                r = m - 1;
            }
        }

        return -1;
    }

    // Возвращает индекс первого значения с ключом равному или большему указанного ключа,
    // если хранилище содержит только ключи меньшие, чем указанный ключ, возвращем -1
    public static long leftBinarySearch(MemorySegment readValuesMS,
                                        MemorySegment readOffsetsMS,
                                        MemorySegment desiredKey,
                                        Comparator<MemorySegment> comparator) {
        long valuesCount = readOffsetsMS.byteSize() / Long.BYTES;
        long l = 0;
        long r = valuesCount - 1;

        while (l < r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Long.BYTES);
            MemorySegment key = getKeyBySizeOffset(readValuesMS, keySizeOffset);

            if (comparator.compare(key, desiredKey) == 0) {
                return m;
            } else if (comparator.compare(key, desiredKey) < 0) {
                l = m + 1;
            } else {
                r = m;
            }
        }

        // Если найденный ключ оказался меньше нужного, то мы говорим, что ничего не нашли
        long keySizeOffset = readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, l * Long.BYTES);
        MemorySegment key = getKeyBySizeOffset(readValuesMS, keySizeOffset);
        if (key != null && comparator.compare(key, desiredKey) < 0) {
            return -1;
        }

        return l;
    }

    // Entry хранятся в следующем виде:
    // |Длина ключа в байтах|Ключ|Длина значения в байтах|Значение|
    public static long writeEntry(Entry<MemorySegment> src, MemorySegment dst, final long fileOffset) {
        long localFileOffset = fileOffset;
        // Сначала пишем длину ключа и сам ключ
        localFileOffset = writeMemorySegment(src.key(), dst, localFileOffset);
        // Потом пишем длину значения и само значение
        localFileOffset = writeMemorySegment(src.value(), dst, localFileOffset);
        return localFileOffset;
    }

    public static long getEntryOffset(Entry<MemorySegment> entry, final long fileOffset) {
        long localFileOffset = fileOffset;
        // Сначала пишем длину ключа и сам ключ
        localFileOffset = getMemorySegmentOffset(entry.key(), localFileOffset);
        // Потом пишем длину значения и само значение
        localFileOffset = getMemorySegmentOffset(entry.value(), localFileOffset);
        return localFileOffset;
    }

    // Записать пару: |Длина MS в байтах|MS|
    public static long writeMemorySegment(MemorySegment value, MemorySegment dst, final long fileOffset) {
        long valueSize = value == null ? TOMBSTONE_VALUE : value.byteSize();
        long localFileOffset = fileOffset;
        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, localFileOffset, valueSize);
        localFileOffset += Long.BYTES;
        if (value != null) {
            MemorySegment.copy(value, 0, dst, localFileOffset, valueSize);
            localFileOffset += valueSize;
        }
        return localFileOffset;
    }

    public static long getMemorySegmentOffset(MemorySegment value, final long fileOffset) {
        long valueSize = value == null ? TOMBSTONE_VALUE : value.byteSize();
        long localFileOffset = fileOffset;
        localFileOffset += Long.BYTES;
        if (value != null) {
            localFileOffset += valueSize;
        }
        return localFileOffset;
    }

    public static long writeEntryOffset(long entryOffset, MemorySegment writeOffsetMS, long fileOffset) {
        writeOffsetMS.set(ValueLayout.JAVA_LONG_UNALIGNED, fileOffset, entryOffset);
        return fileOffset + Long.BYTES;
    }
}
