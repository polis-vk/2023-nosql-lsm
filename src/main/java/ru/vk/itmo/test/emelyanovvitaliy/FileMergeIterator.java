package ru.vk.itmo.test.emelyanovvitaliy;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;

public class FileMergeIterator implements Iterator<Entry<MemorySegment>> {
    private static final Comparator<FileIterator> FILE_ITERATOR_COMPARATOR = new FileIteratorComparator();
    private final Comparator<MemorySegment> memorySegmentComparator;
    private final Queue<FileIterator> fileIteratorQueue;
    private final Iterator<Entry<MemorySegment>> memoryIterator;
    private final MemorySegment keyTo;
    private Entry<MemorySegment> nowMemorySegment;
    private Entry<MemorySegment> storedEntry;

    public FileMergeIterator(
            Comparator<MemorySegment> memorySegmentComparator,
            Queue<FileIterator> fileIteratorQueue,
            Iterator<Entry<MemorySegment>> memoryIterator,
            MemorySegment keyTo
    ) {
        this.memorySegmentComparator = memorySegmentComparator;
        this.fileIteratorQueue = fileIteratorQueue;
        this.memoryIterator = memoryIterator;
        nowMemorySegment = null;
        if (memoryIterator.hasNext()) {
            nowMemorySegment = memoryIterator.next();
        }
        this.keyTo = keyTo;
        storedEntry = null;
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
        Entry<MemorySegment> ans;
        if (storedEntry == null) {
            ans = requireNext();
        } else {
            ans = storedEntry;
            storedEntry = null;
        }
        return ans;
    }

    private Entry<MemorySegment> requireNext() {
        Entry<MemorySegment> entry;
        do {
            entry = privateNext();
        } while (entry != null && entry.value() == null);
        return entry;
    }

    private Entry<MemorySegment> privateNext() {
        FileIterator bestFileIterator = findBestFileIterator();
        Entry<MemorySegment> bestEntry = null;
        if (bestFileIterator != null) {
            bestEntry = bestFileIterator.getNow();
        }
        if (nowMemorySegment != null && bestEntry != null) {
            int compared = memorySegmentComparator.compare(nowMemorySegment.key(), bestEntry.key());
            if (compared <= 0) {
                if (compared == 0) {
                    bestFileIterator.next();
                }
                Entry<MemorySegment> ans = nowMemorySegment;
                moveInMemorySegment();
                return ans;
            }
        }
        if (bestFileIterator == null) {
            Entry<MemorySegment> ans = nowMemorySegment;
            moveInMemorySegment();
            return ans;
        }
        bestFileIterator.next();
        return bestEntry;
    }

    private void moveInMemorySegment() {
        if (memoryIterator.hasNext()) {
            nowMemorySegment = memoryIterator.next();
        } else {
            nowMemorySegment = null;
        }
    }

    private FileIterator findBestFileIterator() {
        Entry<MemorySegment> bestEntry = null;
        FileIterator bestFileIterator = null;
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
                if (FILE_ITERATOR_COMPARATOR.compare(bestFileIterator, fileIterator) < 0) {
                    bestEntry = entry;
                    bestFileIterator.next();
                    bestFileIterator = fileIterator;
                } else {
                    fileIterator.next();
                }
            }
        }
        return bestFileIterator;
    }
}
