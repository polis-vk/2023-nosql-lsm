package ru.vk.itmo.shemetovalexey.sstable;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.shemetovalexey.InMemoryDao;
import ru.vk.itmo.shemetovalexey.MergeIterator;

import java.lang.foreign.MemorySegment;
import java.util.*;

import static ru.vk.itmo.shemetovalexey.sstable.SSTableUtils.*;

public class SSTableIterator {
    public static Iterator<Entry<MemorySegment>> get(List<MemorySegment> segmentList) {
        return get(segmentList,null);
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

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, InMemoryDao::compare)) {
            @Override
            protected boolean shouldSkip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    static Iterator<Entry<MemorySegment>> iterator(MemorySegment page, MemorySegment from, MemorySegment to) {
        long recordIndexFrom = from == null ? 0 : normalize(indexOf(page, from));
        long recordIndexTo = to == null ? recordsCount(page) : normalize(indexOf(page, to));
        long recordsCount = recordsCount(page);

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
                MemorySegment key = slice(page, startOfKey(page, index), endOfKey(page, index));
                long startOfValue = startOfValue(page, index);
                MemorySegment value =
                    startOfValue < 0
                        ? null
                        : slice(page, startOfValue, endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }
}
