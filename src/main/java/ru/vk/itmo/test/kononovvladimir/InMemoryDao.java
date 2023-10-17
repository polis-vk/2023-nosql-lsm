package ru.vk.itmo.test.kononovvladimir;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
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

class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Path dataPath;
    private final Path keyPath;
    private final MemorySegment dataSegment;
    private final MemorySegment keySegment;
    private final Arena dataArena;
    private final Arena keyArena;

    private final Comparator<MemorySegment> memorySegmentComparator = (o1, o2) -> {
        long mismatch = o1.mismatch(o2);
        if (mismatch == -1) return 0;
        else if (mismatch == o1.byteSize()) {
            return -1;
        } else if (mismatch == o2.byteSize()) {
            return 1;
        } else {
            byte b1 = o1.get(ValueLayout.JAVA_BYTE, mismatch);
            byte b2 = o2.get(ValueLayout.JAVA_BYTE, mismatch);
            return Byte.compare(b1, b2);
        }
    };

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> concurrentSkipListMap
            = new ConcurrentSkipListMap<>(memorySegmentComparator);

    public InMemoryDao(Config config) throws IOException {

        String dataFileName = "data.txt";
        String keyFileName = "key.txt";

        dataPath = config.basePath().resolve(dataFileName);
        keyPath = config.basePath().resolve(keyFileName);

        if (Files.exists(dataPath)) {
            try (var fileChanel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
                dataArena = Arena.ofConfined();
                dataSegment = fileChanel.map(FileChannel.MapMode.READ_ONLY, 0, fileChanel.size(), dataArena);
            }
        } else {
            dataArena = null;
            dataSegment = null;
        }

        if (Files.exists(keyPath)) {
            try (var fileChanel = FileChannel.open(keyPath, StandardOpenOption.READ)) {
                keyArena = Arena.ofConfined();
                keySegment = fileChanel.map(FileChannel.MapMode.READ_ONLY, 0, fileChanel.size(), keyArena);
            }
        } else {
            keyArena = null;
            keySegment = null;
        }
    }

    @Override
    public void close() throws IOException {
        if (dataPath != null) {
            Files.deleteIfExists(dataPath);
        }
        if (keyPath != null) {
            Files.deleteIfExists(keyPath);
        }

        if (dataPath == null || keyPath == null) {
            return;
        }

        try (var dataChanel = FileChannel.open(dataPath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            try (var keyChanel = FileChannel.open(keyPath, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {

                long sizeData = 0;
                long sizeKeys = 0;

                for (Entry<MemorySegment> value : concurrentSkipListMap.values()) {
                    sizeData += value.value().byteSize() + Long.BYTES;
                    sizeKeys += value.key().byteSize() + Long.BYTES;
                }

                sizeData += Long.BYTES;

                try (Arena arenaData = Arena.ofConfined()) {
                    try (Arena arenaKeys = Arena.ofConfined()) {

                        MemorySegment dataWriteSegment = dataChanel.map(FileChannel.MapMode.READ_WRITE,
                                0, sizeData, arenaData);
                        MemorySegment keyWriteSegment = keyChanel.map(FileChannel.MapMode.READ_WRITE,
                                0, sizeKeys, arenaKeys);

                        long offsetData = 0;
                        long offsetKeys = 0;

                        dataWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, concurrentSkipListMap.size());
                        offsetData += Long.BYTES;

                        for (Entry<MemorySegment> value : concurrentSkipListMap.values()) {

                            dataWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, value.value().byteSize());
                            keyWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKeys, value.key().byteSize());

                            offsetData += Long.BYTES;
                            offsetKeys += Long.BYTES;

                            MemorySegment.copy(value.value(), 0, dataWriteSegment,
                                    offsetData, value.value().byteSize());
                            MemorySegment.copy(value.key(), 0, keyWriteSegment,
                                    offsetKeys, value.key().byteSize());

                            offsetData += value.value().byteSize();
                            offsetKeys += value.key().byteSize();
                        }
                    }
                }
            }
        } finally {
            if (dataArena != null) {
                dataArena.close();
            }
            if (keyArena != null) {
                keyArena.close();
            }
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> mapValue = concurrentSkipListMap.get(key);
        if (mapValue != null) {
            return mapValue;
        }

        if (dataSegment == null || keySegment == null) {
            return null;
        }

        long dataOffset = 0;
        long keyOffset = 0;

        long sslTableSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        dataOffset += Long.BYTES;

        for (int i = 0; i < sslTableSize; i++) {
            long keySize = keySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
            long dataSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);

            keyOffset += Long.BYTES;
            dataOffset += Long.BYTES;

            MemorySegment keySegmentSlice = keySegment.asSlice(keyOffset, keySize);
            if (segmentsEquals(keySegmentSlice, key)) {
                return new BaseEntry<>(key, dataSegment.asSlice(dataOffset, dataSize));
            }

            keyOffset += keySize;
            dataOffset += dataSize;
        }
        return null;
    }

    private boolean segmentsEquals(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        return memorySegment1.mismatch(memorySegment2) == -1;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return concurrentSkipListMap.values().iterator();
        }

        if (from == null) {
            return concurrentSkipListMap.headMap(to).values().iterator();
        }

        if (to == null) {
            return concurrentSkipListMap.tailMap(from).values().iterator();
        }

        return concurrentSkipListMap.subMap(from, to).values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null || entry.key() == null || entry.value() == null) return;
        concurrentSkipListMap.put(entry.key(), entry);
    }
}
