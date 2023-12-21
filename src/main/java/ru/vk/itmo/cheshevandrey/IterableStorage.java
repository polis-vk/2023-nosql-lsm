package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class IterableStorage implements Iterable<Entry<MemorySegment>> {

    Iterable<Entry<MemorySegment>> iterableMemTable;
    DiskStorage diskStorage;

    IterableStorage(Iterable<Entry<MemorySegment>> iterableMemTable, DiskStorage diskStorage) {
        this.iterableMemTable = iterableMemTable;
        this.diskStorage = diskStorage;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return diskStorage.range(iterableMemTable.iterator(), null, null);
    }
}
