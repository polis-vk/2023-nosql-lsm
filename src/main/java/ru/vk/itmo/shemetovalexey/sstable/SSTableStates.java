package ru.vk.itmo.shemetovalexey.sstable;

import ru.vk.itmo.Entry;
import ru.vk.itmo.shemetovalexey.MemorySegmentComparator;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public final class SSTableStates {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> readStorage;
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> writeStorage;
    private final List<MemorySegment> diskSegmentList;

    private SSTableStates(
        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> readStorage,
        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> writeStorage,
        List<MemorySegment> diskSegmentList
    ) {
        this.readStorage = readStorage;
        this.writeStorage = writeStorage;
        this.diskSegmentList = diskSegmentList;
    }

    private static ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> createMap() {
        return new ConcurrentSkipListMap<>(MemorySegmentComparator::compare);
    }

    public static SSTableStates create(List<MemorySegment> segments) {
        return new SSTableStates(
            createMap(),
            createMap(),
            segments
        );
    }

    public SSTableStates compact(MemorySegment compacted) {
        return new SSTableStates(readStorage, writeStorage, Collections.singletonList(compacted));
    }

    public SSTableStates beforeFlush() {
        return new SSTableStates(writeStorage, createMap(), diskSegmentList);
    }

    public SSTableStates afterFlush(MemorySegment newPage) {
        List<MemorySegment> segments = new ArrayList<>(diskSegmentList.size() + 1);
        segments.addAll(diskSegmentList);
        segments.add(newPage);
        return new SSTableStates(createMap(), writeStorage, segments);
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getReadStorage() {
        return readStorage;
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getWriteStorage() {
        return writeStorage;
    }

    public List<MemorySegment> getDiskSegmentList() {
        return diskSegmentList;
    }
}
