package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.proninvalentin.EnrichedEntry;
import ru.vk.itmo.proninvalentin.Metadata;
import ru.vk.itmo.proninvalentin.utils.MemorySegmentUtils;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class FileIterator {
    private FileIterator() {
    }

    public static Iterator<EnrichedEntry> create(MemorySegment readValuesMS,
                                                 MemorySegment readMetadataMS,
                                                 MemorySegment from,
                                                 MemorySegment to,
                                                 Comparator<MemorySegment> comparator) {
        long startIndex = 0;
        if (from != null) {
            startIndex = MemorySegmentUtils.leftBinarySearch(readValuesMS, readMetadataMS, from, comparator);
            if (startIndex == -1) {
                return Collections.emptyIterator();
            }
        }

        long endIndex = -1;
        if (to != null) {
            endIndex = MemorySegmentUtils.leftBinarySearch(readValuesMS, readMetadataMS, to, comparator);
        }

        if (endIndex == -1) {
            endIndex = readMetadataMS.byteSize() / Metadata.SIZE;
        }

        long finalStartIndex = startIndex;
        long finalEndIndex = endIndex;
        return new Iterator<>() {
            private long curIndex = finalStartIndex;

            @Override
            public boolean hasNext() {
                // Смотрим не дошли ли мы до конца, либо не является ли текущая запись последней перед toKey
                return curIndex < finalEndIndex;
            }

            @Override
            public EnrichedEntry next() {
                Metadata metadata = MemorySegmentUtils.getMetadataByIndex(readMetadataMS, curIndex);
                BaseEntry<MemorySegment> entry = MemorySegmentUtils.getEntryByIndex(
                        readValuesMS, readMetadataMS, curIndex++);
                return new EnrichedEntry(metadata, entry);
            }
        };
    }
}
