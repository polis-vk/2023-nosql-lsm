package ru.vk.itmo.prokopyevnikita;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static ru.vk.itmo.prokopyevnikita.Storage.FILE_PREFIX;

public final class StorageAdditionalFunctionality {
    private StorageAdditionalFunctionality() {
    }

    public static long saveEntrySegment(MemorySegment newSSTable, Entry<MemorySegment> entry, long offsetData) {
        long offset = offsetData;
        newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
        offset += Long.BYTES;

        MemorySegment.copy(entry.key(), 0, newSSTable, offset, entry.key().byteSize());
        offset += entry.key().byteSize();

        if (entry.value() == null) {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
            offset += Long.BYTES;
        } else {
            newSSTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
            offset += Long.BYTES;

            MemorySegment.copy(entry.value(), 0, newSSTable, offset, entry.value().byteSize());
            offset += entry.value().byteSize();
        }
        return offset;
    }

    public static long binarySearchUpperBoundOrEquals(MemorySegment ssTable, MemorySegment key) {
        long left = 0;
        long right = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        if (key == null) {
            return right;
        }
        right--;
        while (left <= right) {
            long mid = (left + right) / 2;

            long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, FILE_PREFIX + Long.BYTES * mid);
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            int cmp = MemorySegmentComparator.compareWithOffsets(
                    ssTable, offset, offset + keySize,
                    key, 0, key.byteSize());
            if (cmp < 0) {
                left = mid + 1;
            } else if (cmp > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    public static Entry<MemorySegment> getEntryByIndex(MemorySegment ssTable, long index) {
        long offset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, FILE_PREFIX + Long.BYTES * index);
        long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        MemorySegment keySegment = ssTable.asSlice(offset + Long.BYTES, keySize);
        offset += Long.BYTES + keySize;

        long valueSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        if (valueSize == -1) {
            return new BaseEntry<>(keySegment, null);
        }

        MemorySegment valueSegment = ssTable.asSlice(offset, valueSize);
        return new BaseEntry<>(keySegment, valueSegment);
    }
}
