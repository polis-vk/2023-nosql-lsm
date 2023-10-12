package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class Utils {
    public static MemorySegment getBySizeOffset(MemorySegment valuesStorage, long sizeOffset) {
        long valueSize = valuesStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, sizeOffset);
        long valueOffset = sizeOffset + Long.BYTES;
        return valuesStorage.asSlice(valueOffset, valueSize);
    }

    public static long binarySearch(MemorySegment valuesStorage,
                              MemorySegment offsetsStorage,
                              MemorySegment desiredKey,
                              Comparator<MemorySegment> comparator) {
        long offsetsCount = offsetsStorage.byteSize() / Long.BYTES;
        long l = 0;
        long r = offsetsCount - 1;

        while (l <= r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = offsetsStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Long.BYTES);
            MemorySegment key = getBySizeOffset(valuesStorage, keySizeOffset);

            if (comparator.compare(key, desiredKey) == 0) {
                return m * Long.BYTES;
            } else if (comparator.compare(key, desiredKey) < 0) {
                l = m + 1;
            } else {
                r = m - 1;
            }
        }

        return -1;
    }

    // Данный бинарный поиск нужен для нахождения первого ключа в файле, который больше либо равен нужному ключу
    public static long leftBinarySearch(MemorySegment valuesStorage,
                                        MemorySegment offsetsStorage,
                                        MemorySegment desiredKey,
                                        Comparator<MemorySegment> comparator) {
        long offsetsCount = offsetsStorage.byteSize() / Long.BYTES;
        long l = 0;
        long r = offsetsCount - 1;

        while (l < r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = offsetsStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Long.BYTES);
            MemorySegment key = getBySizeOffset(valuesStorage, keySizeOffset);

            if (comparator.compare(key, desiredKey) == 0) {
                return m * Long.BYTES;
            } else if (comparator.compare(key, desiredKey) < 0) {
                l = m + 1;
            } else {
                r = m;
            }
        }

        // Если найденный ключ оказался меньше нужного, то мы говорим, что ничего не нашли
        long keySizeOffset = offsetsStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, l * Long.BYTES);
        MemorySegment key = getBySizeOffset(valuesStorage, keySizeOffset);
        if (key != null && comparator.compare(key, desiredKey) < 0) {
            return -1;
        }

        return l * Long.BYTES;
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

    // Записать пару: |Длина значения в байтах|Значение|
    public static long writeMemorySegment(MemorySegment value, MemorySegment dst, final long fileOffset) {
        long valueSize = value.byteSize();
        long localFileOffset = fileOffset;
        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, localFileOffset, valueSize);
        localFileOffset += Long.BYTES;
        MemorySegment.copy(value, 0, dst, localFileOffset, valueSize);
        localFileOffset += valueSize;
        return localFileOffset;
    }

    // Записать оффсет Entry: |Сдвиг значения в байтах от начала файла|
    public static long writeEntryOffset(long offset, MemorySegment dst, long fileOffset) {
        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, fileOffset, offset);
        return fileOffset + Long.BYTES;
    }
}
