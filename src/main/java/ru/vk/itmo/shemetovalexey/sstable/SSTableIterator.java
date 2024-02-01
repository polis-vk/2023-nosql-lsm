package ru.vk.itmo.shemetovalexey.sstable;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.shemetovalexey.MemorySegmentComparator;
import ru.vk.itmo.shemetovalexey.MergeIterator;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public final class SSTableIterator {
    private SSTableIterator() {
    }

    public static Iterator<Entry<MemorySegment>> get(List<MemorySegment> segmentList) {
        return get(segmentList, null);
    }

    public static Iterator<Entry<MemorySegment>> get(List<MemorySegment> segmentList, MemorySegment from) {
        return get(
            Collections.emptyIterator(),
            Collections.emptyIterator(),
            segmentList,
            from,
            null
        );
    }

    public static Iterator<Entry<MemorySegment>> get(
        Iterator<Entry<MemorySegment>> firstIterator,
        Iterator<Entry<MemorySegment>> secondIterator,
        List<MemorySegment> segmentList,
        MemorySegment from,
        MemorySegment to
    ) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);
        iterators.add(secondIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, MemorySegmentComparator::compare)) {
            @Override
            protected boolean shouldSkip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : SSTableUtils.normalize(SSTableUtils.indexOf(page, from));
        long recordIndexTo = to == null ? SSTableUtils.recordsCount(page) : SSTableUtils.normalize(
            SSTableUtils.indexOf(page, to)
        );
        long recordsCount = SSTableUtils.recordsCount(page);

        return new Iterator<>() {
            long index = recordIndexFrom;

            @Override
            public boolean hasNext() {
                return index < recordIndexTo;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                MemorySegment key = SSTableUtils.slice(
                    page,
                    SSTableUtils.startOfKey(page, index),
                    SSTableUtils.endOfKey(page, index)
                );
                long startOfValue = SSTableUtils.startOfValue(page, index);
                MemorySegment value =
                    startOfValue < 0
                        ? null
                        : SSTableUtils.slice(page, startOfValue, SSTableUtils.endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }
}
