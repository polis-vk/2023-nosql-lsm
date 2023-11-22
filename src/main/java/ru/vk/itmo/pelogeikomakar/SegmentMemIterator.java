package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

public class SegmentMemIterator implements SegmentIterInterface{
    private final Iterator<Entry<MemorySegment>> iterator;
    private final AtomicReference<String> atomicName;

    //private DiskStorage storage;
    private final String oldName;
    public SegmentMemIterator(Iterator<Entry<MemorySegment>> iter, String oldNm, AtomicReference<String> atomicName) {
        iterator = iter;
        oldName = oldNm;
        this.atomicName = atomicName;
    }
    @Override
    public AtomicReference<String> getAtomic() {
        return atomicName;
    }

    @Override
    public String getOldName() {
        return oldName;
    }

    @Override
    public void setStorage(DiskStorage storage) {
        //this.storage = storage;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        return iterator.next();
    }
    @Override
    public void readerLock() {

    }

    @Override
    public void readerUnlock() {

    }
}
