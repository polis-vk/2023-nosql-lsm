package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.utils.MemorySegmentUtils;
import ru.vk.itmo.proninvalentin.Metadata;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class FileIterator {
    public static Iterator<Entry<MemorySegment>> create(MemorySegment readValuesMS,
                                                        MemorySegment readOffsetsMS,
                                                        MemorySegment from,
                                                        MemorySegment to,
                                                        Comparator<MemorySegment> comparator,
                                                        long metadataFileSize) {
        long entryIndex = 0;
        if (from != null) {
            entryIndex = MemorySegmentUtils.leftBinarySearch(readValuesMS, readOffsetsMS, from, comparator);
            if (entryIndex == -1) {
                return Collections.emptyIterator();
            }
        }

        long finalEntryIndex = entryIndex;
        return new Iterator<>() {
            private long curIndex = finalEntryIndex;
            private final long valuesCount = metadataFileSize / Metadata.SIZE;

            @Override
            public boolean hasNext() {
                // Смотрим не дошли ли мы до конца, либо не является ли текущая запись последней перед toKey
                if (curIndex < valuesCount) {
                    var key = MemorySegmentUtils.getKeyByIndex(readValuesMS, readOffsetsMS, curIndex);
                    return comparator.compare(key, to) < 0;
                } else {
                    return false;
                }
            }

            @Override
            public Entry<MemorySegment> next() {
                Metadata metadata = MemorySegmentUtils.getMetadataByIndex(readValuesMS, curIndex);
                BaseEntry<MemorySegment> entry = MemorySegmentUtils.getEntryByIndex(
                        readValuesMS, readOffsetsMS, curIndex++);
                return metadata.isDeleted ? null : entry;
            }
        };
    }
}
