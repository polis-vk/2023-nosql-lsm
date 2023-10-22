package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class SkipNullIterator implements Iterator<Entry<MemorySegment>> {

    private final PeekingIterator iterator;
    private final FileManager fileManager;

    public SkipNullIterator(PeekingIterator iterator, FileManager fileManager) {
        this.iterator = iterator;
        this.fileManager = fileManager;
    }

    @Override
    public boolean hasNext() {
        while (iterator.hasNext() && iterator.peek().value() == null) {
            iterator.next();
        }

        if (!iterator.hasNext()) {
            fileManager.clearFileIterators();
        }

        return iterator.hasNext();
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException("There is no next element.");
        }

        Entry<MemorySegment> nextEntry = iterator.next();

        if (!hasNext()) {
            fileManager.clearFileIterators();
        }

        return nextEntry;
    }
}
