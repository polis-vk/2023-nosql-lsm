package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final String SSTABLE = "sstable.txt";

    private final Path ssTablePath;

    private final Arena arena = Arena.ofShared();

    private final MemorySegment mappedMemorySegment;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap
            = new ConcurrentSkipListMap<>(memSegmentComparator);

    private static final MemorySegmentComparator memSegmentComparator = new MemorySegmentComparator();

    private static final class MemorySegmentComparator implements Comparator<MemorySegment> {

        @Override
        public int compare(final MemorySegment a, final MemorySegment b) {
            long mismatchOffset = a.mismatch(b);
            if (mismatchOffset == -1) {
                return 0;
            } else if (mismatchOffset == a.byteSize()) {
                return -1;
            } else if (mismatchOffset == b.byteSize()) {
                return 1;
            } else {
                return Byte.compare(a.getAtIndex(ValueLayout.JAVA_BYTE, mismatchOffset),
                        b.getAtIndex(ValueLayout.JAVA_BYTE, mismatchOffset));
            }
        }
    }

    public InMemoryDao() {
        this.ssTablePath = null;
        this.mappedMemorySegment = null;
    }

    public InMemoryDao(final Config config) {
        this.ssTablePath = config.basePath().resolve(SSTABLE);
        if (!Files.exists(ssTablePath)) {
            this.mappedMemorySegment = null;
            return;
        }
        try (FileChannel fileChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            long ssTableFileSize = Files.size(ssTablePath);
            this.mappedMemorySegment = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY, 0, ssTableFileSize, arena);
        } catch (IOException e) {
            arena.close();
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns ordered iterator of entries with keys between from (inclusive) and to (exclusive).
     *
     * @param from lower bound of range (inclusive)
     * @param to   upper bound of range (exclusive)
     * @return entries [from;to)
     */
    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
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
     * Returns entry by key. Note: default implementation is far from optimal.
     *
     * @param key entry`s key
     * @return entry
     */
    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final Entry<MemorySegment> memorySegmentEntry = memorySegmentMap.get(key);
        if (memorySegmentEntry != null) {
            return memorySegmentEntry;
        }
        if (mappedMemorySegment == null) {
            return null;
        }
        long offset = 0;
        MemorySegment lastValue = null;
        while (offset < mappedMemorySegment.byteSize()) {
            long keyLength = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            if (keyLength != key.byteSize()) {
                offset += keyLength;
                offset += mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset) + Long.BYTES;
                continue;
            }
            final MemorySegment expectedKey = mappedMemorySegment.asSlice(offset, keyLength);
            offset += keyLength;
            long valueLength = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            if (key.mismatch(expectedKey) == -1) {
                lastValue = mappedMemorySegment.asSlice(offset + Long.BYTES, valueLength);
            }
            offset += Long.BYTES + valueLength;
        }
        if (lastValue != null) {
            return new BaseEntry<>(key, lastValue);
        }
        return null;
    }

    /**
     * Inserts of replaces entry.
     *
     * @param entry element to upsert
     */
    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        memorySegmentMap.put(entry.key(), entry);
    }

    private long getSSTableFileSize() {
        long sz = 0;
        for (final Entry<MemorySegment> entry : memorySegmentMap.values()) {
            sz += entry.key().byteSize() + entry.value().byteSize() + 2 * Long.BYTES;
        }
        return sz;
    }

    @Override
    public void close() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(ssTablePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofConfined()) {
            long fileSize = getSSTableFileSize();
            final MemorySegment writeMemorySegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, fileSize, writeArena);
            long offset = 0;
            for (final Entry<MemorySegment> entry : memorySegmentMap.values()) {
                offset = writeMemorySegment(entry.key(), writeMemorySegment, offset);
                offset = writeMemorySegment(entry.value(), writeMemorySegment, offset);
            }
            writeMemorySegment.load();
        } finally {
            arena.close();
        }
    }

    private long writeMemorySegment(final MemorySegment srcMemorySegment,
                                    final MemorySegment dstMemorySegment,
                                    final long offset) {
        long srcMemorySegmentSize = srcMemorySegment.byteSize();
        dstMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, srcMemorySegmentSize);
        MemorySegment.copy(srcMemorySegment, 0, dstMemorySegment, offset + Long.BYTES,
                srcMemorySegmentSize);
        return offset + Long.BYTES + srcMemorySegmentSize;
    }
}
