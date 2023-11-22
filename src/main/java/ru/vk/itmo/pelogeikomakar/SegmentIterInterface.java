package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

public interface SegmentIterInterface extends Iterator<Entry<MemorySegment>> {
    public AtomicReference<String> getAtomic();
    public void readerLock();
    public void readerUnlock();

    public String getOldName();
    public void setStorage(DiskStorage storage);
}
