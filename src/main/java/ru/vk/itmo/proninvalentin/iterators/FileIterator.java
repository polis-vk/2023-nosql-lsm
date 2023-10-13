package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.Constants;
import ru.vk.itmo.proninvalentin.MemorySegmentUtils;

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
        long entryOffset = 0;
        if (from != null) {
            entryOffset = MemorySegmentUtils.leftBinarySearch(readValuesMS, readOffsetsMS, from, comparator);
            if (entryOffset == -1) {
                return Collections.emptyIterator();
            }
        }

        long finalEntryOffset = entryOffset;
        return new Iterator<>() {
            private long curIndex = finalEntryOffset == 0 ? 0 : finalEntryOffset / Constants.METADATA_SIZE;
            private final long valuesCount = metadataFileSize / Constants.METADATA_SIZE;
            private final MemorySegment toKey = to.asSlice(0, Constants.METADATA_SIZE);

            @Override
            public boolean hasNext() {
                // Смотрим не дошли ли мы до конца, либо не является ли текущая запись последней перед toKey
                if (curIndex + 1 < valuesCount) {
                    var key = MemorySegmentUtils.getKeyByIndex(readValuesMS,
                            readOffsetsMS,
                            curIndex + 1);
                    return comparator.compare(key, toKey) < 0;
                } else {
                    return false;
                }
            }

            @Override
            public Entry<MemorySegment> next() {
                curIndex++;
                return MemorySegmentUtils.getEntryByIndex(readValuesMS, readOffsetsMS, curIndex + 1);
            }
        };
    }
}
