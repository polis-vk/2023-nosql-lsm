package ru.vk.itmo.proninvalentin.utils;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.Metadata;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public final class MemorySegmentUtils {
    private MemorySegmentUtils() {
    }

    public static MemorySegment getBySizeOffset(MemorySegment readValuesMS, long sizeOffset) {
        long valueSize = readValuesMS.get(ValueLayout.JAVA_LONG_UNALIGNED, sizeOffset);
        long valueOffset = sizeOffset + Long.BYTES;
        return readValuesMS.asSlice(valueOffset, valueSize);
    }

    public static BaseEntry<MemorySegment> getEntryByIndex(MemorySegment readValuesMS,
                                                           MemorySegment readMetadataMS,
                                                           long index) {
        Metadata metadata = getMetadataByIndex(readMetadataMS, index);
        MemorySegment key = getBySizeOffset(readValuesMS, metadata.entryOffset);
        long valueSizeOffset = metadata.entryOffset + Long.BYTES + key.byteSize();
        if (metadata.isDeleted) {
            return new BaseEntry<>(MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)), null);
        } else {
            MemorySegment value = getBySizeOffset(readValuesMS, valueSizeOffset);
            return new BaseEntry<>(
                    MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)),
                    MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)));
        }
    }

    public static Metadata getMetadataByIndex(MemorySegment readMetadataMS,
                                              long index) {
        long entryOffset = index * Metadata.SIZE;
        long isDeletedOffset = entryOffset + Metadata.ENTRY_OFFSET_SIZE;
        long createdAtOffset = isDeletedOffset + Metadata.IS_DELETED_SIZE;
        return new Metadata(
                readMetadataMS.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset),
                readMetadataMS.get(ValueLayout.JAVA_BYTE, isDeletedOffset) == 1,
                readMetadataMS.get(ValueLayout.JAVA_LONG_UNALIGNED, createdAtOffset));
    }

    // Возвращает индекс значения с указанным ключом, если он есть, иначе возвращает -1
    public static long binarySearch(MemorySegment readValuesMS,
                                    MemorySegment readMetadataMS,
                                    MemorySegment desiredKey,
                                    Comparator<MemorySegment> comparator) {
        long valuesCount = readMetadataMS.byteSize() / Metadata.SIZE;
        long l = 0;
        long r = valuesCount - 1;

        while (l <= r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = readMetadataMS.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Metadata.SIZE);
            MemorySegment key = getBySizeOffset(readValuesMS, keySizeOffset);

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
                                        MemorySegment readMetadataMS,
                                        MemorySegment desiredKey,
                                        Comparator<MemorySegment> comparator) {
        long valuesCount = readMetadataMS.byteSize() / Metadata.SIZE;
        long l = 0;
        long r = valuesCount - 1;

        while (l < r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = readMetadataMS.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Metadata.SIZE);
            MemorySegment key = getBySizeOffset(readValuesMS, keySizeOffset);

            if (comparator.compare(key, desiredKey) == 0) {
                return m;
            } else if (comparator.compare(key, desiredKey) < 0) {
                l = m + 1;
            } else {
                r = m;
            }
        }

        // Если найденный ключ оказался меньше нужного, то мы говорим, что ничего не нашли
        long keySizeOffset = readMetadataMS.get(ValueLayout.JAVA_LONG_UNALIGNED, l * Metadata.SIZE);
        MemorySegment key = getBySizeOffset(readValuesMS, keySizeOffset);
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
        if (src.value() != null) {
            localFileOffset = writeMemorySegment(src.value(), dst, localFileOffset);
        }
        return localFileOffset;
    }

    // Записать пару: |Длина MS в байтах|MS|
    public static long writeMemorySegment(MemorySegment value, MemorySegment dst, final long fileOffset) {
        long valueSize = value.byteSize();
        long localFileOffset = fileOffset;
        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, localFileOffset, valueSize);
        localFileOffset += Long.BYTES;
        MemorySegment.copy(value, 0, dst, localFileOffset, valueSize);
        localFileOffset += valueSize;
        return localFileOffset;
    }

    public static long writeEntryMetadata(long entryOffset, boolean isDeleted, long createdAt,
                                          MemorySegment writeMetadataMS, long metadataOffset) {
        long isDeletedOffset = metadataOffset + Metadata.ENTRY_OFFSET_SIZE;
        long createdAtOffset = isDeletedOffset + Metadata.IS_DELETED_SIZE;
        writeMetadataMS.set(ValueLayout.JAVA_LONG_UNALIGNED, metadataOffset, entryOffset);
        writeMetadataMS.set(ValueLayout.JAVA_BOOLEAN, isDeletedOffset, isDeleted);
        writeMetadataMS.set(ValueLayout.JAVA_LONG_UNALIGNED, createdAtOffset, createdAt);
        return metadataOffset + Metadata.SIZE;
    }
}
