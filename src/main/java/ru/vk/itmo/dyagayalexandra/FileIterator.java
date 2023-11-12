package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FileIterator implements Iterator<Entry<MemorySegment>> {

    private final MemorySegment ssTable;
    private final MemorySegment ssIndex;
    private long currentIndex;
    private final long endIndex;
    private final FileManager fileManager;

    public FileIterator(MemorySegment ssTable, MemorySegment ssIndex, MemorySegment from,
                        MemorySegment to, long indexSize, FileManager fileManager) throws IOException {
        this.ssTable = ssTable;
        this.ssIndex = ssIndex;
        currentIndex = from == null ? 0 : fileManager.getEntryIndex(ssTable, ssIndex, from, indexSize);
        endIndex = to == null ? indexSize : fileManager.getEntryIndex(ssTable, ssIndex, to, indexSize);
        this.fileManager = fileManager;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < endIndex;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new IllegalStateException("No more entries in the table.");
        }

        Entry<MemorySegment> entry;
        try {
            entry = fileManager.getCurrentEntry(currentIndex, ssTable, ssIndex);
        } catch (IOException e) {
            throw new NoSuchElementException("There is no next element.", e);
        }

        currentIndex++;
        return entry;
    }
}
