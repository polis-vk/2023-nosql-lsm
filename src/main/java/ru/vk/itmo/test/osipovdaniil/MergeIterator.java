package ru.vk.itmo.test.osipovdaniil;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public class MergeIterator implements Iterator<Entry<MemorySegment>> {


    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap;

    final ArrayList<SavingIterator> iterators = new ArrayList<>();
    public MergeIterator(final int numFiles,
                         final Path SSTablesPath,
                         final MemorySegment from,
                         final MemorySegment to,
                         final NavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap) {
        this.memorySegmentMap = memorySegmentMap;
        iterators.add(new SaveMapIterator(getFromMap(from, to)));
        addIterators(numFiles, SSTablesPath, from, to);
    }

    public void addIterators(final int numFiles,
                             final Path SSTablesPath,
                             final MemorySegment from,
                             final MemorySegment to) {
        for (int i = numFiles - 1; i >= 0; --i) {
            final Path filePath = SSTablesPath.resolve(i + Utils.SSTABLE);
            try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
                long ssTableFileSize = Files.size(filePath);
                final MemorySegment mappedMemorySegment = channel.map(
                        FileChannel.MapMode.READ_ONLY, 0, ssTableFileSize, Arena.ofShared());
                iterators.add(new FileDaoIterator(mappedMemorySegment, from, to));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public Iterator<Entry<MemorySegment>> getFromMap(final MemorySegment from, final MemorySegment to) {
        if (from == null && to == null) {
            return memorySegmentMap.values().iterator();
        } else if (from == null) {
            return memorySegmentMap.headMap(to).values().iterator();
        } else if (to == null) {
            return memorySegmentMap.tailMap(from).values().iterator();
        } else {
            return memorySegmentMap.subMap(from, to).values().iterator();
        }
    }

    /**
     * Returns {@code true} if the iteration has more elements.
     * (In other words, returns {@code true} if {@link #next} would
     * return an element rather than throwing an exception.)
     *
     * @return {@code true} if the iteration has more elements
     */
    @Override
    public boolean hasNext() {
        for (final Iterator<Entry<MemorySegment>> iterator : iterators) {
            if (iterator.hasNext()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the next element in the iteration.
     *
     * @return the next element in the iteration
     * @throws NoSuchElementException if the iteration has no more elements
     */
    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Entry<MemorySegment> minNextEntry = null;
        boolean first = true;
        for (final SavingIterator iterator : iterators) {
            if (iterator.getCurrEntry() != null) {
                Entry<MemorySegment> currEntry = iterator.getCurrEntry();
                if (first) {
                    minNextEntry = currEntry;
                    first = false;
                    continue;
                }
                if (Utils.compareMemorySegments(currEntry.key(), minNextEntry.key()) < 0) {
                    minNextEntry = currEntry;
                }
            }
        }
        if (minNextEntry == null) {
            throw new RuntimeException("min Next Entry cannot be null");
        }
        Entry<MemorySegment> res = null;
        for (final SavingIterator iterator : iterators) {
            if (iterator.getCurrEntry() != null) {
                Entry<MemorySegment> currEntry = iterator.getCurrEntry();
                if (minNextEntry.key().mismatch(currEntry.key()) == -1) {
                    if (res == null) {
                        res = currEntry;
                    }
                    iterator.next();
                }
            }
        }
        return res;
    }
}
