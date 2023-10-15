package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.EnrichedEntry;
import ru.vk.itmo.proninvalentin.utils.MemorySegmentUtils;
import ru.vk.itmo.proninvalentin.Metadata;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class FileIterator {
    public static Iterator<EnrichedEntry> create(MemorySegment readValuesMS,
                                                 MemorySegment readOffsetsMS,
                                                 MemorySegment from,
                                                 MemorySegment to,
                                                 Comparator<MemorySegment> comparator,
                                                 long metadataFileSize) {
        long startIndex = 0;
        if (from != null) {
            startIndex = MemorySegmentUtils.leftBinarySearch(readValuesMS, readOffsetsMS, from, comparator);
            if (startIndex == -1) {
                return Collections.emptyIterator();
            }
        }

        long endIndex;
        if (to != null) {
            endIndex = MemorySegmentUtils.leftBinarySearch(readValuesMS, readOffsetsMS, to, comparator);
            if (endIndex == -1) {
                endIndex = metadataFileSize / Metadata.SIZE;
            }
        } else {
            endIndex = metadataFileSize / Metadata.SIZE;
        }

        long finalStartIndex = startIndex;
        long finalEndIndex = endIndex;
        return new Iterator<>() {
            private long curIndex = finalStartIndex;

            @Override
            public boolean hasNext() {
                /*// Пропускаем все удаленные Entry
                while (curIndex < finalEndIndex
                        && MemorySegmentUtils.getMetadataByIndex(readOffsetsMS, curIndex).isDeleted) {
                    curIndex++;
                }
                // Смотрим не дошли ли мы до конца, либо не является ли текущая запись последней перед toKey*/
                return curIndex < finalEndIndex;
            }

            @Override
            public EnrichedEntry next() {
                Metadata metadata = MemorySegmentUtils.getMetadataByIndex(readOffsetsMS, curIndex);
                BaseEntry<MemorySegment> entry = MemorySegmentUtils.getEntryByIndex(
                        readValuesMS, readOffsetsMS, curIndex++);
                return new EnrichedEntry(metadata, entry);
            }
        };
    }
}
