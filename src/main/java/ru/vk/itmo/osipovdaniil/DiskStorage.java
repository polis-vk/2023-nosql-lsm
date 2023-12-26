package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DiskStorage {

    private final List<MemorySegment> segmentList;

    public DiskStorage(final List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
    }

    static Iterator<Entry<MemorySegment>> iterator(final MemorySegment page,
                                                   final MemorySegment from,
                                                   final MemorySegment to) {
        long recordIndexFrom = from == null
                ? 0 : DiskStorageUtilsSimple.normalize(DiskStorageUtils.indexOf(page, from));
        long recordIndexTo = to == null
                ? DiskStorageUtilsSimple.recordsCount(page) :
                DiskStorageUtilsSimple.normalize(DiskStorageUtils.indexOf(page, to));
        final long recordsCount = DiskStorageUtilsSimple.recordsCount(page);
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
                MemorySegment key = DiskStorageUtilsSimple.slice(page,
                        DiskStorageUtilsSimple.startOfKey(page, index),
                        DiskStorageUtilsSimple.endOfKey(page, index));
                long startOfValue = DiskStorageUtilsSimple.startOfValue(page, index);
                MemorySegment value =
                        startOfValue < 0
                                ? null
                                : DiskStorageUtilsSimple.slice(page, startOfValue,
                                DiskStorageUtilsSimple.endOfValue(page, index, recordsCount));
                index++;
                return new BaseEntry<>(key, value);
            }
        };
    }

    public Iterator<Entry<MemorySegment>> range(
            final Iterator<Entry<MemorySegment>> firstIterator,
            final MemorySegment from,
            final MemorySegment to) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(segmentList.size() + 1);
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(iterator(memorySegment, from, to));
        }
        iterators.add(firstIterator);

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, Utils::compareMemorySegments)) {
            @Override
            protected boolean skip(final Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }
}
