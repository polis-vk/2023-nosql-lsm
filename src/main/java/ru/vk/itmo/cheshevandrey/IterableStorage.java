package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Iterator;

public class IterableStorage implements Iterable<Entry<MemorySegment>> {
    DiskStorage diskStorage;
    Range range;

    IterableStorage(DiskStorage diskStorage, Range range) {
        this.diskStorage = diskStorage;
        this.range = range;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return diskStorage.range(
                Collections.emptyIterator(),
                Collections.emptyIterator(),
                null,
                null,
                range
        );
    }
}