package ru.vk.itmo.osipovdaniil;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.osipovdaniil.EntryBoolPair;
import ru.vk.itmo.test.osipovdaniil.MergeIterator;
import ru.vk.itmo.test.osipovdaniil.Utils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private int numSavedSSTables = 0;

    private final Path ssTablesPath;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memorySegmentMap
            = new ConcurrentSkipListMap<>(Utils::compareMemorySegments);


    public InMemoryDao() {
        this.ssTablesPath = null;
    }

    public InMemoryDao(final Config config) {
        this.ssTablesPath = config.basePath().resolve(Utils.SSTABLE);
        while (Files.exists(ssTablesPath.resolve(numSavedSSTables + Utils.SSTABLE))) {
            numSavedSSTables++;
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
        return new MergeIterator(numSavedSSTables, ssTablesPath, from, to, memorySegmentMap);
    }

    /**
     * Returns entry by key. Note: default implementation is far from optimal.
     *
     * @param key entry`s key
     * @return entry
     */
    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        if (memorySegmentMap.containsKey(key)) {
            return memorySegmentMap.get(key);
        }
        for (int i = numSavedSSTables - 1; i >= 0; --i) {
            EntryBoolPair res = findInFile(ssTablesPath.resolve(numSavedSSTables + Utils.SSTABLE), key);
            if (res.trueVal()) {
                return res.entry();
            }
        }
        return null;
    }

    EntryBoolPair findInFile(final Path filePath, final MemorySegment key) {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            long ssTableFileSize = Files.size(filePath);
            final MemorySegment mappedMemorySegment = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, ssTableFileSize, Arena.ofShared());
            final long entriesOffset = Long.BYTES + Long.BYTES * 2
                    * mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            long minOffsetOffset = Long.BYTES;
            long maxOffsetOffset = entriesOffset;
            while (maxOffsetOffset - minOffsetOffset > Long.BYTES * 2) {
                long mOffsetOffset = (maxOffsetOffset + minOffsetOffset) / 2;
                long keySegOff = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, mOffsetOffset);
                long valSegOff = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, mOffsetOffset + Long.BYTES);
                int cmp = Utils.compareMemorySegments(key,
                        mappedMemorySegment.asSlice(keySegOff, valSegOff - keySegOff));
                if (cmp == 0) {
                    final long nextKeyOffset = (mOffsetOffset + 2 * Long.BYTES == entriesOffset) ?
                            mappedMemorySegment.byteSize() : mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                            mOffsetOffset + 2 * Long.BYTES);
                    final MemorySegment key1 = mappedMemorySegment.asSlice(keySegOff, valSegOff - keySegOff);
                    final MemorySegment value = mappedMemorySegment.asSlice(valSegOff, nextKeyOffset - valSegOff);
                    return new EntryBoolPair(new BaseEntry<>(key1, value), true);
                }
                if (Utils.compareMemorySegments(key,
                        mappedMemorySegment.asSlice(keySegOff, valSegOff - keySegOff)) < 0) {
                    maxOffsetOffset = mOffsetOffset;
                } else {
                    minOffsetOffset = mOffsetOffset;
                }
            }
            long keySegOff = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, minOffsetOffset);
            long valSegOff = mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, minOffsetOffset + Long.BYTES);
            final MemorySegment key1 = mappedMemorySegment.asSlice(keySegOff, valSegOff - keySegOff);
            if (key1.mismatch(key) == -1) {
                final long nextKeyOffset = (minOffsetOffset + 2 * Long.BYTES == entriesOffset) ?
                        mappedMemorySegment.byteSize() : mappedMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED,
                        minOffsetOffset + 2 * Long.BYTES);
                final MemorySegment value = mappedMemorySegment.asSlice(valSegOff, nextKeyOffset - valSegOff);
                return new EntryBoolPair(new BaseEntry<>(key1, value), true);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new EntryBoolPair(null, false);
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
        long sz = Integer.BYTES;
        for (final Entry<MemorySegment> entry : memorySegmentMap.values()) {
            sz += entry.key().byteSize() + entry.value().byteSize() + 2 * Integer.BYTES;
        }
        return sz;
    }

    private long writeOffsets(final MemorySegment dstMemorySegment) {
        long offset = 0;
        dstMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, memorySegmentMap.size());
        offset += Integer.BYTES;
        long entriesOffset = offset + Long.BYTES * memorySegmentMap.size() * 2L;
        for (final Entry<MemorySegment> entry : memorySegmentMap.values()) {
            dstMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entriesOffset);
            offset += Integer.BYTES;
            entriesOffset += entry.key().byteSize();
            dstMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entriesOffset);
            offset += Integer.BYTES;
            entriesOffset += entry.value().byteSize();
        }
        return offset;
    }

    @Override
    public void flush() throws IOException {
        try (FileChannel fileChannel = FileChannel.open(ssTablesPath.resolve(numSavedSSTables + Utils.SSTABLE),
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.CREATE);
             Arena writeArena = Arena.ofShared()) {
            numSavedSSTables++;
            long fileSize = getSSTableFileSize();
            final MemorySegment mappedMemorySegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, fileSize, writeArena);
            long offset = writeOffsets(mappedMemorySegment);
            for (final Entry<MemorySegment> entry : memorySegmentMap.values()) {
                offset = writeMemorySegment(entry.key(), mappedMemorySegment, offset);
                offset = writeMemorySegment(entry.value(), mappedMemorySegment, offset);
            }
            mappedMemorySegment.load();

        }
    }

    @Override
    public void close() throws IOException {
        if (!memorySegmentMap.isEmpty()) {
            flush();
        }
    }

    private long writeMemorySegment(final MemorySegment srcMemorySegment,
                                    final MemorySegment dstMemorySegment,
                                    final long offset) {
        long srcMemorySegmentSize = srcMemorySegment.byteSize();
        MemorySegment.copy(srcMemorySegment, 0, dstMemorySegment, offset, srcMemorySegmentSize);
        return offset + srcMemorySegmentSize;
    }
}
