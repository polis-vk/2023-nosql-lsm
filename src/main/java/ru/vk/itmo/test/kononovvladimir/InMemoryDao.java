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
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    public static class ComboFiles {
        Path index;
        Path data;
        Path key;
        long id;

        public ComboFiles(Path index, Path data, Path key, String id) {
            this.index = index;
            this.data = data;
            this.key = key;
            this.id = Long.parseLong(id);
        }
    }
    private Path dataPath;
    private Path keyPath;
    private Path indexPath;
    private MemorySegment indexSegment;
    private MemorySegment dataSegment;
    private MemorySegment keySegment;
    private IndexSearcher indexSearcher;
    private DataSearcher dataSearcher;
    private KeySearcher keySearcher;
    private Arena dataArena;
    private Arena keyArena;
    private Arena indexArena;
    long numberNulls = 0;

    private final Config config;
    private long size = 0;

    private final Set<ComboFiles> filesSet = new ConcurrentSkipListSet<>(Comparator.comparingLong(o -> o.id));


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
    @SuppressWarnings("StringSplitter")
    public InMemoryDao(Config config) throws IOException {
        this.config = config;

        if (Files.exists(config.basePath())) {
            Files.walkFileTree(
                    config.basePath(),
                    Set.of(),
                    2,
                    new SimpleFileVisitor<>() {
                        @Override
                        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                            if (file.getFileName().toString().startsWith("index")) {
                                String[] ids = file.getFileName().toString().split("\\$");
                                String id = ids[1];
                                Path dataPath1 = config.basePath().resolve("data$" + id + "$.txt");
                                Path keyPath1 = config.basePath().resolve("key$" + id + "$.txt");
                                filesSet.add(new ComboFiles(file, dataPath1, keyPath1, id));
                            }
                            return FileVisitResult.CONTINUE;
                        }
                    }
            );
        }
/*        String dataFileName = "data.txt";
        String keyFileName = "key.txt";
        String indexFileName = "index.txt";*/
        ComboFiles comboFiles = null;
        if (filesSet.iterator().hasNext()) {
            comboFiles = filesSet.iterator().next();
        }
        if (comboFiles == null) {
            return;
        }
        indexPath = comboFiles.index;
        dataPath = comboFiles.data;
        keyPath = comboFiles.key;
        Arena arena = Arena.ofShared();
        if (Files.exists(indexPath)) {
            try (var fileChanel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                indexArena = arena;
                indexSegment = fileChanel.map(FileChannel.MapMode.READ_ONLY, 0, fileChanel.size(), indexArena);
                indexSearcher = new IndexSearcher(indexSegment);
                size = indexSearcher.getSslSize();
            }
        } else {
            indexArena = null;
            indexSegment = null;
            indexSearcher = null;
        }


        if (Files.exists(dataPath)) {
            try (var fileChanel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
                dataArena = arena;
                dataSegment = fileChanel.map(FileChannel.MapMode.READ_ONLY, 0, fileChanel.size(), dataArena);
                dataSearcher = new DataSearcher(dataSegment, size);

            }
        } else {
            dataArena = null;
            dataSegment = null;
            dataSearcher = null;
        }

        if (Files.exists(keyPath)) {
            try (var fileChanel = FileChannel.open(keyPath, StandardOpenOption.READ)) {
                keyArena = arena;
                keySegment = fileChanel.map(FileChannel.MapMode.READ_ONLY, 0, fileChanel.size(), keyArena);
                keySearcher = new KeySearcher(keySegment, size);
            }
        } else {
            keyArena = null;
            keySegment = null;
            keySearcher = null;
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

        String currentMillis = Long.toString(System.currentTimeMillis());
        String indexTempName = "index$" + currentMillis + "$.txt";
        String dataTempName = "data$" + currentMillis + "$.txt";
        String keyTempName = "key$" + currentMillis + "$.txt";

        Path indexTempPath = config.basePath().resolve(indexTempName);
        Path dataTempPath = config.basePath().resolve(dataTempName);
        Path keyTempPath = config.basePath().resolve(keyTempName);
/*        if (dataPath == null || keyPath == null || indexPath == null) {
            return;
        }*/

/*        Path filePath = sstablesPath.resolve(
                Path.of(
                        Long.toString(System.currentTimeMillis(), Character.MAX_RADIX)
                                + Long.toString(System.nanoTime(), Character.MAX_RADIX)
                                + ".sstable"
                )
        );*/

        //String tempFileDataName = "tempData.txt";
        //String tempFileKeyName = "tempKey.txt";
        //Path pathTempData = config.basePath().resolve(tempFileDataName);
        //Path pathTempKey = config.basePath().resolve(tempFileKeyName);

        //String index = "index.txt";
        //Path pathIndex = config.basePath().resolve(index);
        //long sslTableSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        try (var dataChanel = FileChannel.open(dataTempPath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            try (var keyChanel = FileChannel.open(keyTempPath, StandardOpenOption.READ,
                    StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                try (var indexChanel = FileChannel.open(indexTempPath, StandardOpenOption.READ,
                        StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
                    long sizeData = 0;
                    long sizeKeys = 0;
                    long sizeIndex = 2 * Long.BYTES + Long.BYTES * (long) concurrentSkipListMap.size() * 3L; // size map

                    for (Entry<MemorySegment> value : concurrentSkipListMap.values()) {
                        MemorySegment valueSegment = value.value();
                        if (valueSegment == null){
                            sizeData += Long.BYTES;
                        } else {
                            sizeData += value.value().byteSize() + Long.BYTES;
                        }
                        sizeKeys += value.key().byteSize() + Long.BYTES;
                    }


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
                                indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, numberNulls);
                                offsetIndex += Long.BYTES;
                                long indexNum = 0;

                                for (Entry<MemorySegment> value : concurrentSkipListMap.values()) {
                                    long startOffsetData = offsetData;
                                    long startOffsetKey = offsetKeys;

                                    MemorySegment valueSegment = value.value();
                                    if (valueSegment == null) {
                                        dataWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, -1);
                                    } else {
                                        dataWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, valueSegment.byteSize());
                                    }
                                    keyWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKeys, value.key().byteSize());
                                    indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, indexNum++);

                                    offsetData += Long.BYTES;
                                    offsetKeys += Long.BYTES;
                                    offsetIndex += Long.BYTES;

                                    if (valueSegment != null) {
                                        MemorySegment.copy(valueSegment, 0, dataWriteSegment,
                                                offsetData, valueSegment.byteSize());
                                        offsetData += value.value().byteSize();
                                    }
                                    MemorySegment.copy(value.key(), 0, keyWriteSegment,
                                            offsetKeys, value.key().byteSize());

                                    indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, startOffsetData);
                                    offsetIndex += Long.BYTES;
                                    indexWriteSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, startOffsetKey);

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
/*        File oldFile = new File(indexPath.toUri().getPath());
        if (oldFile.exists()) {
            oldFile.delete();
        }
        new File(indexPath.toUri().getPath()).renameTo(oldFile);

        File oldFile1 = new File(dataPath.toUri().getPath());
        if (oldFile1.exists()) {
            oldFile1.delete();
        }
        new File(dataTempPath.toUri().getPath()).renameTo(oldFile1);

        File oldFile2 = new File(keyPath.toUri().getPath());
        if (oldFile2.exists()) {
            oldFile2.delete();
        }
        new File(keyTempPath.toUri().getPath()).renameTo(oldFile2);*/
        //Files.move(indexTempPath, indexPath, StandardCopyOption.REPLACE_EXISTING);
        //Files.move(dataTempPath, dataPath, StandardCopyOption.REPLACE_EXISTING);
        //Files.move(keyTempPath, keyPath, StandardCopyOption.REPLACE_EXISTING);

    }

    private long binSearch(MemorySegment key, long sslTableSize) {
        long index = -1;
        long low = 0;
        long high = sslTableSize - 1;
        while (low <= high) {
            long mid = low + (high - low) / 2;
            //long indexOffset = Long.BYTES + 3L * mid * Long.BYTES;
            long offsetKeyMid = indexSearcher.getKeyOffset(mid);
            keySearcher.goToOffset(offsetKeyMid, 0);
            MemorySegment memorySegmentKeyMid = keySearcher.getValueInStrokeAndGo();
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
        //Entry<MemorySegment> mapValue = concurrentSkipListMap.get(key);
        if (concurrentSkipListMap.containsKey(key)) {
            if (concurrentSkipListMap.get(key).value() != null) {
                return concurrentSkipListMap.get(key);
            }
            return null;
        }

        if (dataSegment == null || keySegment == null || indexSegment == null) {
            return null;
        }

        //long indexOffset = 0;
        long sslTableSize = indexSearcher.getSslSize();
        //indexOffset += Long.BYTES;
        long indexResult = binSearch(key, sslTableSize);
        if (indexResult == -1) return null;
        long dataOffset = indexSearcher.getDataOffset(indexResult);
        dataSearcher.goToOffset(dataOffset, 0);
        MemorySegment dataMemorySegment = dataSearcher.getValueInStrokeAndGo();
        if (dataMemorySegment == null) return null;
        return new BaseEntry<>(key, dataMemorySegment);
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
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        long startInFile = -1;
        long lastInFile = -1;
        long sizeFile = 0;
        if (indexSegment != null) {
            sizeFile = size;
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
            return new Iterator<>() {
                Entry<MemorySegment> peek;

                @Override
                public boolean hasNext() {
                    if (peek == null || peek.value() == null) {
                        if (inMemoryIterator.hasNext()) {
                            peek = inMemoryIterator.next();
                            return hasNext();
                        } else {
                            return false;
                        }
                    }
                    return peek != null;
                }

                @Override
                public Entry<MemorySegment> next() {
                    if (hasNext()) {
                        Entry<MemorySegment> res = peek;
                        peek = null;
                        return res;
                    } else {
                        return null;
                    }
                }
            };
        }


        //optimize
        for (long i = 0; i < sizeFile; i++) {

            long keyOffset = indexSearcher.getKeyOffset(i);

            keySearcher.goToOffset(keyOffset, 0);
            MemorySegment memorySegmentKey = keySearcher.getValueInStrokeAndGo();
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
        try {
            return new InFileIterator(new SslFilesIterator(indexPath, dataPath, keyPath, sizeFile), startInFile,
                    lastInFile + 1, inMemoryIterator, memorySegmentComparator);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            numberNulls++;
        }
        if (concurrentSkipListMap.containsKey(entry.key()) && concurrentSkipListMap.get(entry.key()).value() == null) {
            numberNulls--;
        }
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
