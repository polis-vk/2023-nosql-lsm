package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;


public class FileIterator implements Iterator<Entry<MemorySegment>> {

    private final FileChannel channelTable;
    private final FileChannel channelIndex;
    private final RandomAccessFile rafTable;
    private final RandomAccessFile rafIndex;
    private long currentIndex;
    private final long endIndex;

    public FileIterator(Path ssTable, Path ssIndex, MemorySegment from, MemorySegment to, long indexSize) throws IOException {
        rafTable = new RandomAccessFile(String.valueOf(ssTable), "r");
        rafIndex = new RandomAccessFile(String.valueOf(ssIndex), "r");
        channelTable = rafTable.getChannel();
        channelIndex = rafIndex.getChannel();

        currentIndex = from == null ? 0 : FileManager.getEntryIndex(channelTable, channelIndex, from, indexSize);
        endIndex = to == null ? indexSize : FileManager.getEntryIndex(channelTable, channelIndex, to, indexSize);
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
            entry = FileManager.getCurrentEntry(currentIndex, channelTable, channelIndex);
        } catch (IOException e) {
            throw new NoSuchElementException("There is no next element.", e);
        }
        currentIndex++;
        return entry;
    }

    public void close() throws IOException {
        channelTable.close();
        channelIndex.close();
        rafTable.close();
        rafIndex.close();
    }
}
