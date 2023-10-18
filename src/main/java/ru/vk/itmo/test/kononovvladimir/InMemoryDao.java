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
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Path dataPath;
    private final Path keyPath;
    private final Path indexPath;

    private final MemorySegment indexSegment;
    private final MemorySegment dataSegment;
    private final MemorySegment keySegment;
    private final Arena dataArena;
    private final Arena keyArena;
    private final Arena indexArena;

    //private final Config config;

    private final Comparator<MemorySegment> memorySegmentComparator = (o1, o2) -> {
        if (o1 == null && o2 == null) {
            return 0;
        }
        if (o1 == null) {
            return -1;
        }
        if (o2 == null) {
            return 1;
        }
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
        //this.config = config;

        String dataFileName = "data.txt";
        String keyFileName = "key.txt";
        String indexFileName = "index.txt";
        indexPath = config.basePath().resolve(indexFileName);
        dataPath = config.basePath().resolve(dataFileName);
        keyPath = config.basePath().resolve(keyFileName);
        Arena arena = Arena.ofShared();
        if (Files.exists(indexPath)) {
            try (var fileChanel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                indexArena = arena;
                indexSegment = fileChanel.map(FileChannel.MapMode.READ_ONLY, 0, fileChanel.size(), indexArena);
            }
        } else {
            indexArena = null;
            indexSegment = null;
        }

        if (Files.exists(dataPath)) {
            try (var fileChanel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
                dataArena = arena;
                dataSegment = fileChanel.map(FileChannel.MapMode.READ_ONLY, 0, fileChanel.size(), dataArena);
            }
        } else {
            dataArena = null;
            dataSegment = null;
        }

        if (Files.exists(keyPath)) {
            try (var fileChanel = FileChannel.open(keyPath, StandardOpenOption.READ)) {
                keyArena = arena;
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
            //Files.deleteIfExists(dataPath);
        }
        if (keyPath != null) {
            //Files.deleteIfExists(keyPath);
        }

        if (dataPath == null || keyPath == null || indexPath == null) {
            return;
        }

        //String tempFileDataName = "tempData.txt";
        //String tempFileKeyName = "tempKey.txt";
        //Path pathTempData = config.basePath().resolve(tempFileDataName);
        //Path pathTempKey = config.basePath().resolve(tempFileKeyName);

        //String index = "index.txt";
        //Path pathIndex = config.basePath().resolve(index);
        //long sslTableSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);

        try (var dataChanel = FileChannel.open(dataPath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            try (var keyChanel = FileChannel.open(keyPath, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                try (var indexChanel = FileChannel.open(indexPath, StandardOpenOption.READ,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                    long sizeData = 0;
                    long sizeKeys = 0;
                    long sizeIndex = 0;

                    for (Entry<MemorySegment> value : concurrentSkipListMap.values()) {
                        sizeData += value.value().byteSize() + Long.BYTES;
                        sizeKeys += value.key().byteSize() + Long.BYTES;
                    }

                    sizeIndex += Long.BYTES + Long.BYTES * (long) concurrentSkipListMap.size() * 3L; // size map

                    try (Arena arenaData = Arena.ofConfined()) {
                        try (Arena arenaKeys = Arena.ofConfined()) {
                            try (Arena arenaIndex = Arena.ofConfined()) {
                                MemorySegment dataWriteSegment = dataChanel.map(FileChannel.MapMode.READ_WRITE,
                                        0, sizeData, arenaData);
                                MemorySegment keyWriteSegment = keyChanel.map(FileChannel.MapMode.READ_WRITE,
                                        0, sizeKeys, arenaKeys);
                                MemorySegment indexWriteSegment = indexChanel.map(FileChannel.MapMode.READ_WRITE,
                                        0, sizeIndex, arenaIndex);

                                long offsetData = 0;
                                long offsetKeys = 0;
                                long offsetIndex = 0;

                                indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, concurrentSkipListMap.size());
                                offsetIndex += Long.BYTES;
                                long indexNum = 0;

                                for (Entry<MemorySegment> value : concurrentSkipListMap.values()) {
                                    long startOffsetData = offsetData;
                                    long startOffsetKey = offsetKeys;

                                    dataWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, value.value().byteSize());
                                    keyWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKeys, value.key().byteSize());
                                    indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, indexNum++);

                                    offsetData += Long.BYTES;
                                    offsetKeys += Long.BYTES;
                                    offsetIndex += Long.BYTES;

                                    MemorySegment.copy(value.value(), 0, dataWriteSegment,
                                            offsetData, value.value().byteSize());
                                    MemorySegment.copy(value.key(), 0, keyWriteSegment,
                                            offsetKeys, value.key().byteSize());

                                    indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, startOffsetData);
                                    offsetIndex += Long.BYTES;
                                    indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, startOffsetKey);

                                    offsetData += value.value().byteSize();
                                    offsetKeys += value.key().byteSize();
                                    offsetIndex += Long.BYTES;

                                }

                            }
                        }
                    }
                }
            }
        } finally {
            if (indexArena != null) {
                indexArena.close();
            }
/*            if (dataArena != null) {
                dataArena.close();
            }
            if (keyArena != null) {
                keyArena.close();
            }*/
        }
    }

    private long binSearch(MemorySegment key, long sslTableSize) {
        long index = -1;
        long low = 0;
        long high = sslTableSize - 1;
        while (low <= high) {
            long mid = low + (high - low) / 2;
            //long indexOffset = Long.BYTES + 3L * mid * Long.BYTES;
            long offsetKeyMid = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * 3L * mid + (1 + 2) * Long.BYTES);
            long keySize = keySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetKeyMid);
            offsetKeyMid += Long.BYTES;
            MemorySegment memorySegmentKeyMid = keySegment.asSlice(offsetKeyMid, keySize);
            int compare = memorySegmentComparator.compare(memorySegmentKeyMid, key);
            if (compare < 0) {
                low = mid + 1;
            } else if (compare > 0) {
                high = mid - 1;
            } else {
                index = mid;
                break;
            }
        }
        return index;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> mapValue = concurrentSkipListMap.get(key);
        if (mapValue != null) {
            return mapValue;
        }

        if (dataSegment == null || keySegment == null || indexSegment == null) {
            return null;
        }

        //long indexOffset = 0;

        long sslTableSize = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        //indexOffset += Long.BYTES;
        long indexResult = binSearch(key, sslTableSize);
        if (indexResult == -1) return null;
        long dataOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * 3L * indexResult + Long.BYTES + Long.BYTES);
        long dataSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
        dataOffset += Long.BYTES;
        return new BaseEntry<>(key, dataSegment.asSlice(dataOffset, dataSize));
/*
        for (int i = 0; i < sslTableSize; i++) {
            indexOffset += Long.BYTES;
            long dataOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
            indexOffset += Long.BYTES;
            long keyOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
            indexOffset += Long.BYTES;
            long dataSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
            long keySize = keySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);

            keyOffset += Long.BYTES;
            dataOffset += Long.BYTES;

            MemorySegment keySegmentSlice = keySegment.asSlice(keyOffset, keySize);
            if (segmentsEquals(keySegmentSlice, key)) {
                return new BaseEntry<>(key, dataSegment.asSlice(dataOffset, dataSize));
            }

        }
        return null;*/
    }

/*    private boolean segmentsEquals(MemorySegment memorySegment1, MemorySegment memorySegment2) {
        return memorySegment1.mismatch(memorySegment2) == -1;
    }*/

    @Override
    public synchronized Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        long startInFile = -1;
        long lastInFile = -1;
        long sizeFile = 0;
        if (indexSegment != null) {
            sizeFile = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        }
        Iterator<Entry<MemorySegment>> inMemoryIterator;
        if (from == null && to == null) {
            startInFile = 0;
            lastInFile = sizeFile - 1;
            inMemoryIterator = concurrentSkipListMap.values().iterator();
        } else if (from == null) {
            startInFile = 0;
            inMemoryIterator = concurrentSkipListMap.headMap(to).values().iterator();
        } else if (to == null) {
            lastInFile = sizeFile - 1;
            inMemoryIterator = concurrentSkipListMap.tailMap(from).values().iterator();
        } else {
            inMemoryIterator = concurrentSkipListMap.subMap(from, to).values().iterator();
        }

        if (indexSegment == null || dataSegment == null || keySegment == null) {
            return inMemoryIterator;
        }


        //optimize
        for (long i = 0; i < sizeFile; i++) {
            long keyOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES * 3L * i + Long.BYTES + Long.BYTES * 2);
            long keySize = keySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
            keyOffset += Long.BYTES;
            MemorySegment memorySegmentKey = keySegment.asSlice(keyOffset, keySize);
            if (startInFile == -1) {
                long compare = memorySegmentComparator.compare(memorySegmentKey, from);
                if (compare >= 0) {
                    startInFile = i;
                }
            }

            long compare = memorySegmentComparator.compare(memorySegmentKey, to);
            if (lastInFile != sizeFile - 1 && compare < 0) {
                lastInFile = i;
            }
        }
        if (startInFile == -1) {
            return inMemoryIterator;
        }
        return new

                InFileIterator(indexSegment, dataSegment, keySegment, startInFile,
                lastInFile, inMemoryIterator, memorySegmentComparator);

    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry == null || entry.key() == null || entry.value() == null) return;
        concurrentSkipListMap.put(entry.key(), entry);
    }

/*    public static void main(String[] args) throws IOException {
        Arena arena = Arena.ofConfined();
        try (var dataChanel = FileChannel.open(Path.of("C:\\Users\\Volodya\\IdeaProjects\\2023-nosql-lsm\\src\\main\\java\\ru\\vk\\itmo\\test\\kononovvladimir\test.txt"), StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            MemorySegment dataWriteSegment = dataChanel.map(FileChannel.MapMode.READ_WRITE,
                    0, 1000, arena);
            dataWriteSegment.set(ValueLayout.JAVA_INT, 5, 17);
        }

    }*/
}
