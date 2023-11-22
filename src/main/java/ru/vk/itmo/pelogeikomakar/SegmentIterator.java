package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;

import static ru.vk.itmo.pelogeikomakar.DiskStorage.*;

public class SegmentIterator implements SegmentIterInterface {
    long index;
    long recordIndexTo;
    MemorySegment page;
    long recordsCount;
    private String oldName;
    private AtomicReference<String> currentNameAtomic;
    private Lock readLock;
    private final DiskStorage storage;
    public Entry<MemorySegment> nextVal;
    private MemorySegment to;
    public SegmentIterator(MemorySegment from, MemorySegment to, String oldNm, AtomicReference<String> atomic,
                           Lock lock, DiskStorage storage) {
        this.storage = storage;
        this.to = to;

        setState(from, oldNm, atomic, lock);

        nextVal = null;
        prepareNext();

    }

    public void setState(MemorySegment from, String name, AtomicReference<String> atomic,
                    Lock lock) {
        this.page = storage.getSegment(name);
        long recordIndexFrom = from == null ? 0 : normalize(indexOf(page, from));
        long recordIndexTo = to == null ? recordsCount(page) : normalize(indexOf(page, to));
        long recordsCount = recordsCount(page);
        this.index = recordIndexFrom;
        this.recordIndexTo = recordIndexTo;
        this.recordsCount = recordsCount;
        oldName = name;
        currentNameAtomic = atomic;
        readLock = lock;
    }

    public void prepareNext() {
        if (!hasNext()) {
            nextVal = null;
            return;
        }
        MemorySegment key;
        MemorySegment value;
        key = slice(page, startOfKey(page, index), endOfKey(page, index));
        long startOfValue = startOfValue(page, index);
        value = startOfValue < 0 ? null
                : slice(page, startOfValue, endOfValue(page, index, recordsCount));

        index++;
        nextVal = new BaseEntry<>(key, value);
    }

    @Override
    public boolean hasNext() {
        if (nextVal != null) {
            return true;
        }
        return index < recordIndexTo;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        var outVal = nextVal;

        String newName;
        readLock.lock();
        try{
            newName = currentNameAtomic.getAcquire();
        } finally {
            readLock.unlock();
        }
        while (!newName.equals(oldName)) {
            setState(nextVal.key(), newName, storage.getAtomicForTable(newName),
                    storage.getReadLockForTable(newName));
            readLock.lock();
            try {
                newName = currentNameAtomic.getAcquire();
            } finally {
                readLock.unlock();
            }
        }
        readLock.lock();
        try {
            // twice to go through outVal
            prepareNext();
            prepareNext();
        } finally {
            readLock.unlock();
        }

        return outVal;
    }
    private static MemorySegment slice(MemorySegment page, long start, long end) {
        return page.asSlice(start, end - start);
    }

    @Override
    public AtomicReference<String> getAtomic() {
        return currentNameAtomic;
    }

    @Override
    public void readerLock() {
        readLock.lock();
    }

    @Override
    public void readerUnlock() {
        readLock.unlock();
    }

    @Override
    public String getOldName() {
        return oldName;
    }

    @Override
    public void setStorage(DiskStorage storage) {

    }


}
