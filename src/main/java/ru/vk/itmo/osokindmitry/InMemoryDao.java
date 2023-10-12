package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = InMemoryDao::compare;

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memTable
            = new ConcurrentSkipListMap<>(comparator);

    private final Arena arena;
    private final Path path;
    private static final String FILE_NAME = "sstable.txt";

    private long keyLength;
    private long valueLength;


    public InMemoryDao() {
        path = Path.of("C:\\Users\\dimit\\AppData\\Local\\Temp");
        arena = Arena.ofConfined();
    }

    public InMemoryDao(Config config) {
        path = config.basePath().resolve(FILE_NAME);
        arena = Arena.ofConfined();
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memTable.get(key);

        Set<StandardOpenOption> openOptions = Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ);
        if (entry == null && path.toFile().exists()) {

            try (FileChannel fc = FileChannel.open(path, openOptions)) {
                if (fc.size() != 0) {
                    MemorySegment mappedSegment = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size(), arena);
                    entry = binarySearch(mappedSegment, key);
                }
            } catch (IOException e) {
                return null;
            }
        }

        return entry;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return memTable.values().iterator();
        } else if (from == null) {
            return memTable.headMap(to).values().iterator();
        } else if (to == null) {
            return memTable.tailMap(from).values().iterator();
        }

        return memTable.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (keyLength < entry.key().byteSize()) {
            keyLength = entry.key().byteSize();
        }
        if (valueLength < entry.value().byteSize()) {
            valueLength = entry.value().byteSize();
        }
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        try (
                FileChannel fc = FileChannel.open(
                        path,
                        Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
        ) {

            MemorySegment ssTable = fc.map(FileChannel.MapMode.READ_WRITE, 0, getSsTableSize(), arena);
            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, keyLength);
            long a = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            System.out.println(a);
            ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, keyLength, valueLength);
            flushFromSorted(ssTable, 0, memTable.size(), memTable.values().iterator(), keyLength + valueLength);
        }
    }

    private long flushFromSorted(MemorySegment ssTable, int lo, int hi, Iterator<Entry<MemorySegment>> it, long offset) {

        if (hi < lo) {
            throw new IllegalArgumentException();
        }

        int mid = (lo + hi) >>> 1;

        long changedOffset = offset;
        if (lo < mid) {
            changedOffset = flushFromSorted(ssTable, lo, mid - 1, it, offset);
        }

        if (it.hasNext()) {
            Entry<MemorySegment> middle = it.next();

            long srcKeyLength = middle.key().byteSize();
            MemorySegment.copy(
                    middle.key(),
                    0,
                    ssTable,
                    changedOffset + (keyLength - srcKeyLength),
//                    changedOffset,
                    srcKeyLength
            );
            for (long i = changedOffset; i < changedOffset +keyLength - srcKeyLength; i++) {
                ssTable.set(ValueLayout.JAVA_BYTE, changedOffset, (byte)0);
            }
            long c = compare(ssTable.asSlice(changedOffset, keyLength), middle.key());
            System.out.println(c);

            changedOffset += keyLength;
            long srcValueLength = middle.value().byteSize();
            MemorySegment.copy(
                    middle.value(),
                    0,
                    ssTable,
                    changedOffset + (valueLength - srcValueLength),
                    srcValueLength
            );
            c = compare(ssTable.asSlice(changedOffset, valueLength), middle.value());
            System.out.println(c);

            changedOffset += valueLength;
        }


        if (mid < hi) {
            changedOffset = flushFromSorted(ssTable, mid + 1, hi, it, changedOffset);
        }
        return changedOffset;
    }

    @Override
    public void close() throws IOException {
        flush();
        if (!arena.scope().isAlive()) {
            arena.close();
        }
//        try (
//                FileChannel fc = FileChannel.open(
//                        path,
//                        Set.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))
//        ) {
//
//            long ssTableSize = Long.BYTES * 2L * storage.size();
//            for (Entry<MemorySegment> value : storage.values()) {
//                ssTableSize += value.key().byteSize() + value.value().byteSize();
//            }
//
//            MemorySegment ssTable = fc.map(FileChannel.MapMode.READ_WRITE, 0, ssTableSize, arena);
//            long offset = 0;
//
//            for (Entry<MemorySegment> value : storage.values()) {
//                ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.key().byteSize());
//                offset += Long.BYTES;
//                MemorySegment.copy(value.key(), 0, ssTable, offset, value.key().byteSize());
//                offset += value.key().byteSize();
//
//                ssTable.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.value().byteSize());
//                offset += Long.BYTES;
//                MemorySegment.copy(value.value(), 0, ssTable, offset, value.value().byteSize());
//                offset += value.value().byteSize();
//            }
//            arena.close();
//        }
    }


    private long getSsTableSize() {
        return (keyLength + valueLength) * memTable.size() + keyLength + valueLength;
    }

    private Entry<MemorySegment> binarySearch(MemorySegment mappedSegment, MemorySegment key) {
//        long size = (mappedSegment.byteSize() - keyLength - valueLength) / (keyLength + valueLength);
//        long keyLength = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        long keyLength = 7;
        long valueLength = mappedSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyLength);
        long lo = keyLength + valueLength;
        long hi = mappedSegment.byteSize() - (keyLength + valueLength);

        while (lo < hi) {
            long mid = ((hi - lo) >>> 1) + lo;
            mid = mid - mid % (keyLength + valueLength);
            MemorySegment slicedKey = mappedSegment.asSlice(mid, keyLength);
            int diff = compare(slicedKey, key);
            if (diff < 0) {
                lo = mid + keyLength + valueLength;
            } else if (diff > 0) {
                hi = mid;
            } else {
                return new BaseEntry<>(key, mappedSegment.asSlice(mid + keyLength, valueLength));
            }
        }
        return null;
    }

    private static int compare(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        byte[] a = segment1.toArray(ValueLayout.JAVA_BYTE);
        System.out.println(Arrays.toString(a));
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        byte b1 = segment1.get(ValueLayout.JAVA_BYTE, offset);
        byte b2 = segment2.get(ValueLayout.JAVA_BYTE, offset);
        return Byte.compare(b1, b2);
    }

}
