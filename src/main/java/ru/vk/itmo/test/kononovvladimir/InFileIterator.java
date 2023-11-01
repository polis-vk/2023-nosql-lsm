package ru.vk.itmo.test.kononovvladimir;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.Iterator;

public class InFileIterator implements Iterator<Entry<MemorySegment>> {
    Entry<MemorySegment> lastInMemoryEntry;

    Entry<MemorySegment> lastInFileEntry;

    SslFilesIterator sslFilesIterator;

    public InFileIterator(SslFilesIterator sslFilesIterator, long current, long to, Iterator<Entry<MemorySegment>> inMemoryIterator, Comparator<MemorySegment> memorySegmentComparator) {
        this.sslFilesIterator = sslFilesIterator;
        this.current = current;
        this.to = to;
        this.inMemoryIterator = inMemoryIterator;
        this.memorySegmentComparator = memorySegmentComparator;
        nextInMemoryEntry();
        nextInFileEntry();
    }

    long current;
    long to;
    Iterator<Entry<MemorySegment>> inMemoryIterator;
    Comparator<MemorySegment> memorySegmentComparator;


    @Override
    public boolean hasNext() {
/*        if (inMemoryIterator.hasNext() && lastInMemoryEntry == null) {
            nextInMemoryEntry();
        }*/
        if (lastInMemoryEntry == null && lastInFileEntry == null) {
            return false;
        }
        if (lastInMemoryEntry == null) {
            if (lastInFileEntry.value() == null) {
                nextInFileEntry();
                return hasNext();
            }
            return true;
        } else if (lastInFileEntry == null) {
            if (lastInMemoryEntry.value() == null) {
                nextInMemoryEntry();
                return hasNext();
            }
            return true;
        }
        long compare = memorySegmentComparator.compare(lastInMemoryEntry.key(), lastInFileEntry.key());
        if (compare == 0) {
            nextInFileEntry();
            return hasNext();
        }
        if (compare < 0 && lastInMemoryEntry.value() == null){
            nextInMemoryEntry();
            return hasNext();
        }
        if (compare > 0 && lastInFileEntry.value() == null) {
            nextInFileEntry();
            return hasNext();
        }
        return true;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (lastInFileEntry == null) {
            Entry<MemorySegment> res = lastInMemoryEntry;
            nextInMemoryEntry();
            return res;
        }

        if (lastInMemoryEntry == null) {
            Entry<MemorySegment> res = lastInFileEntry;
            nextInFileEntry();
            return res;
        }

        MemorySegment memorySegmentInMemoryKey = lastInMemoryEntry.key();
        long compare = memorySegmentComparator.compare(memorySegmentInMemoryKey, lastInFileEntry.key());
        Entry<MemorySegment> result;
        if (compare <= 0) {
            if (compare == 0) {
                nextInFileEntry();
            }
            result = lastInMemoryEntry;
            nextInMemoryEntry();
        } else {
            result = lastInFileEntry;
            nextInFileEntry();
        }
        return result;
    }

    private void nextInMemoryEntry() {
        if (inMemoryIterator.hasNext()) {
            lastInMemoryEntry = inMemoryIterator.next();
        } else {
            lastInMemoryEntry = null;
        }
    }

    private void nextInFileEntry() {
        if (current < to) {
            long dataOffset = sslFilesIterator.indexSearcher.getDataOffset(current);
            long keyOffset = sslFilesIterator.indexSearcher.getKeyOffset(current);
            sslFilesIterator.goToOffset(dataOffset, keyOffset);

            MemorySegment memorySegmentInFileData = sslFilesIterator.dataSearcher.getValueInStrokeAndGo();
            MemorySegment memorySegmentInFileKey = sslFilesIterator.keySearcher.getValueInStrokeAndGo();
            lastInFileEntry = new BaseEntry<>(memorySegmentInFileKey, memorySegmentInFileData);
            current++;
        } else {
            lastInFileEntry = null;
        }
    }
}
