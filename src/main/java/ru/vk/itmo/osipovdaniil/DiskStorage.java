package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static ru.vk.itmo.osipovdaniil.DiskStorageUtils.iterator;

public class DiskStorage {
    private final List<MemorySegment> segmentList;

    public DiskStorage(final List<MemorySegment> segmentList) {
        this.segmentList = segmentList;
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
