package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import javax.xml.stream.events.EntityReference;
import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

public class FileMergeIterator implements Iterator<Entry<MemorySegment>> {
    private final Comparator<MemorySegment> memorySegmentComparator;
    private final Queue<FileIterator> fileIteratorQueue;
    private final Iterator<Entry<MemorySegment>> memoryIterator;

    private Entry<MemorySegment> nowMemorySegment = null;
    private Entry<MemorySegment> storedEntry = null;
    private MemorySegment keyTo = null;

    public FileMergeIterator(
            Comparator<MemorySegment> memorySegmentComparator,
            Queue<FileIterator> fileIteratorQueue,
            Iterator<Entry<MemorySegment>> memoryIterator,
            MemorySegment keyTo
    ) {
        this.memorySegmentComparator = memorySegmentComparator;
        this.fileIteratorQueue = fileIteratorQueue;
        this.memoryIterator = memoryIterator;
        if (memoryIterator.hasNext()) {
            nowMemorySegment = memoryIterator.next();
        }
        this.keyTo = keyTo;
    }

    @Override
    public boolean hasNext() {
        if (storedEntry != null) {
            return true;
        }
        Entry<MemorySegment> entry = requireNext();
        if (entry == null) {
            return false;
        }
        storedEntry = entry;
        return true;
    }

    @Override
    public Entry<MemorySegment> next() {
        Entry<MemorySegment> ans = null;
        if (storedEntry != null) {
            ans = storedEntry;
            storedEntry = null;
            return ans;
        } else {
            ans = requireNext();
        }
        return ans;
    }

    private Entry<MemorySegment> requireNext() {
        Entry<MemorySegment> entry = null;
        do {
            entry = privateNext();
        } while (entry != null && entry.value() == null);
        return entry;
    }

    private Entry<MemorySegment> privateNext() {
        FileIterator bestFileIterator = null;
        Entry<MemorySegment> bestEntry = null;
        for (FileIterator fileIterator: fileIteratorQueue) {
            if (!fileIterator.hasNow()) {
                continue;
            }
            Entry<MemorySegment> entry = fileIterator.getNow();
            if (keyTo != null && memorySegmentComparator.compare(entry.key(), keyTo) >= 0) {
                continue;
            }
            if (bestEntry == null) {
                bestEntry = entry;
                bestFileIterator = fileIterator;
                continue;
            }
            int compared = memorySegmentComparator.compare(entry.key(), bestEntry.key());
            if (compared < 0) {
                bestEntry = entry;
                bestFileIterator = fileIterator;
            } else if (compared == 0) {
                if (fileIterator.getTimestamp() > bestFileIterator.getTimestamp()) {
                    bestEntry = entry;
                    bestFileIterator.next();
                    bestFileIterator = fileIterator;
                } else {
                    fileIterator.next();
                }
            }
        }
        if (nowMemorySegment != null && bestEntry != null) {
            int compared = memorySegmentComparator.compare(nowMemorySegment.key(), bestEntry.key());
            if (compared <= 0) {
                if (compared == 0) {
                    bestFileIterator.next();
                }
                Entry<MemorySegment> ans = nowMemorySegment;
                if (memoryIterator.hasNext()) {
                    nowMemorySegment = memoryIterator.next();
                } else {
                    nowMemorySegment = null;
                }
                return ans;
            }
        }
        if (bestFileIterator != null) {
            bestFileIterator.next();
            return bestEntry;
        } else {
            Entry<MemorySegment> ans = nowMemorySegment;
            if (memoryIterator.hasNext()) {
                nowMemorySegment = memoryIterator.next();
            } else {
                nowMemorySegment = null;
            }
            return ans;
        }
    }
}
