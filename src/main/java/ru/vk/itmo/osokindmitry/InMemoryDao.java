package ru.vk.itmo.osokindmitry;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static java.nio.file.StandardOpenOption.*;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final Comparator<MemorySegment> comparator = InMemoryDao::compare;


    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> storage
            = new ConcurrentSkipListMap<>(comparator);

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> mappedStorage
            = new ConcurrentSkipListMap<>(comparator);

    private FileChannel fc = null;
    private Path path;

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
//        Set<OpenOption> opts = Set.of(CREATE, READ, WRITE);
//        try {
//            fc = FileChannel.open(config.basePath(), opts);
//            Arena arena = Arena.ofConfined();
//
//            MemorySegment mapped = fc.map(READ_WRITE, 0, 1L << 32, arena);
//
//        } catch (IOException ignored) {
//
//        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = storage.get(key);
        if (entry == null) {
            entry = mappedStorage.get(key);
            if (entry == null && searchFor(key)) {
                entry = mappedStorage.get(key);
            }
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
                Files.write(path, longToBytes(value.key().byteSize()), CREATE, WRITE, APPEND);
                Files.write(path, value.key().toArray(ValueLayout.JAVA_BYTE), CREATE, WRITE, APPEND);

                Files.write(path, longToBytes(value.key().byteSize()), CREATE, WRITE, APPEND);
                Files.write(path, value.value().toArray(ValueLayout.JAVA_BYTE), CREATE, WRITE, APPEND);


            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        // memtable -> sstable
    }

    private boolean searchFor(MemorySegment key) {
        Set<OpenOption> opts = Set.of(CREATE, READ, WRITE);
        Arena arena = Arena.ofAuto();
        try (FileChannel fc = FileChannel.open(path, opts)) {
            // читаем пока файл не закончится или сколько раз указано в переменной специальной

            ByteBuffer buffer = ByteBuffer.allocate(8);
            fc.read(buffer);

            long size = buffer.getLong();
            MemorySegment mappedKey = fc.map(READ_WRITE, 0, size, arena);

            buffer = ByteBuffer.allocate(8);
            fc.read(buffer);
            size = buffer.getLong();
            MemorySegment mappedValue = fc.map(READ_WRITE, 0, size, arena);

            mappedStorage.put(mappedKey, new BaseEntry<>(mappedKey, mappedValue));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
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
