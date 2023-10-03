package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = InMemoryDao::compare;


    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage
            = new ConcurrentSkipListMap<>(comparator);

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> mappedStorage
            = new ConcurrentSkipListMap<>(comparator);

    private FileChannel fc;
    private OutputStream outputStream;
    private final Path path;

    private static int compare(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
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


    public InMemoryDao(Config config) {
        path = config.basePath();
        Set<OpenOption> opts = Set.of(CREATE, READ, WRITE);
        try (FileChannel fc = FileChannel.open(config.basePath(), opts)) {
            if (fc.size() != 0) {
                Arena arena = Arena.ofAuto();
                MemorySegment mappedSegment = fc.map(READ_WRITE, 0, fc.size(), arena);
                sliceSegment(mappedSegment);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry == null) {
            entry = mappedStorage.get(key);
        }
        return entry;
        // взять ключ из постоянного хранилища
        // storageP.get(key);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (storage.isEmpty()) {
            return Collections.emptyIterator();
        }
        boolean empty = to == null;
        MemorySegment first = from == null ? storage.firstKey() : from;
        MemorySegment last = to == null ? storage.lastKey() : to;
        return storage.subMap(first, true, last, empty).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        storage.put(entry.key(), entry);
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        storage.values().forEach((value) -> {
            try {
                Files.write(path, value.key().toArray(ValueLayout.JAVA_BYTE), CREATE, WRITE, APPEND);
                Files.write(path, value.value().toArray(ValueLayout.JAVA_BYTE), CREATE, WRITE, APPEND);

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        // memtable -> sstable
    }

    private void sliceSegment(MemorySegment segment) {
//        long n = segment.get(ValueLayout.JAVA_LONG, 0);
//        List<Long> sizes = new ArrayList<>();
//        for (long i = 0; i < n; i++) {
//            sizes.add(segment.get(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG.byteSize() * i));
//        }
//        for (long i = 0; i < n; i++) {
//            mappedStorage.put(segment.asSlice(sizes.get(i * 2)))
//        }

        long offset = 0;
        while (offset < segment.byteSize() - ValueLayout.JAVA_LONG.byteSize()) {

            long size = segment.get(ValueLayout.JAVA_LONG, offset);
            offset += ValueLayout.JAVA_LONG.byteSize();
            MemorySegment key = segment.asSlice(offset, size);
            offset += size;

            size = segment.get(ValueLayout.JAVA_LONG, offset);
            offset += ValueLayout.JAVA_LONG.byteSize();
            MemorySegment value = segment.asSlice(offset, size);
            offset += size;

            mappedStorage.put(key, new BaseEntry<>(key, value));
        }

    }

    private static byte[] longToBytes(long l) {
        byte[] result = new byte[Long.BYTES];
        for (int i = Long.BYTES - 1; i >= 0; i--) {
            result[i] = (byte) (l & 0xFF);
            l >>= Byte.SIZE;
        }
        return result;
    }

    private static long bytesToLong(final byte[] b) {
        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Byte.SIZE;
            result |= (b[i] & 0xFF);
        }
        return result;
    }

}
