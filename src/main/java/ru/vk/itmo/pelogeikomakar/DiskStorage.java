package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class DiskStorage {

    public static final String SSTABLE_PREFIX = "sstable_";
    private final ConcurrentMap<String, AtomicReference<String>> tablesNames = new ConcurrentSkipListMap<>();
    private final ConcurrentMap<String, ReentrantReadWriteLock> tablesLocks = new ConcurrentSkipListMap<>();
    private final ConcurrentHashMap<String, MemorySegment> segments;
    private final List<String> tablesOverCompacting;
    private volatile boolean isCompacting;
    private final ReadWriteLock compactLock = new ReentrantReadWriteLock();
    private final Lock compactReadLock = compactLock.readLock();
    private final Lock compactWriteLock = compactLock.writeLock();
    private final Path storagePath;
    private final Arena arena;

    public DiskStorage(ConcurrentHashMap<String, MemorySegment> segments, Path storagePath, Arena arena) {
        this.arena = arena;
        this.storagePath = storagePath;
        this.segments = segments;
        isCompacting = false;
        tablesOverCompacting = new ArrayList<>();
        for (String tableName : segments.keySet()) {
            tablesNames.put(tableName, new AtomicReference<>(tableName));
            tablesLocks.put(tableName, new ReentrantReadWriteLock());
        }
    }

    public Iterator<Entry<MemorySegment>> range(
            SegmentIterInterface firstIterator,
            MemorySegment from,
            MemorySegment to) {
        compactReadLock.lock();
        try {
            return getRange(firstIterator, from, to);
        } finally {
            compactReadLock.unlock();
        }
    }
    private MergeIterator getRange(SegmentIterInterface firstIterator,
                                   MemorySegment from,
                                   MemorySegment to) {
        List<SegmentIterInterface> iterators = new ArrayList<>(segments.size() + 1);
        for (String name : segments.keySet()) {
            iterators.add(iterator(name, from, to));
        }
        if (firstIterator != null) {
            iterators.add(firstIterator);
        }

        return new MergeIterator(iterators, Comparator.comparing(Entry::key, ConcurrentDao::compare)) {
            @Override
            protected boolean shouldSkip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    public void saveNextSSTable(Path storagePath, Iterable<Entry<MemorySegment>> iterable,
                                MemoryStorage storage, AtomicReference<String> newName, boolean reopen)
            throws IOException {
        final Path indexTmp = storagePath.resolve("index.tmp");
        final Path indexFile = storagePath.resolve("index.idx");

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }
        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);

        //long lastNum = Long.parseLong(existedFiles.get(existedFiles.size() - 1).split("_")[1]);
        long lastNum = -1;
        if (!existedFiles.isEmpty()) {
            String baseLast = existedFiles.get(existedFiles.size() - 1);
            lastNum = Long.parseLong(baseLast.substring(baseLast.indexOf("_") + 1));
        }
        String newFileName = SSTABLE_PREFIX + (lastNum + 1);

        long dataSize = 0;
        long count = 0;
        for (Entry<MemorySegment> entry : iterable) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(
                        storagePath.resolve(newFileName),
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : iterable) {
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }

            // data:
            // |key0|value0|key1|value1|...
            dataOffset = indexSize;
            for (Entry<MemorySegment> entry : iterable) {
                MemorySegment key = entry.key();
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();
                if (value != null) {
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
            }
        }

        compactWriteLock.lock();
        try {
            if (isCompacting) {
                tablesOverCompacting.add(newFileName);
            }
            List<String> list = new ArrayList<>(existedFiles.size() + 1);
            list.addAll(existedFiles);
            list.add(newFileName);
            Files.write(
                    indexTmp,
                    list,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            );
            Files.deleteIfExists(indexFile);
            Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        } finally {
            compactWriteLock.unlock();
        }
        if (reopen) {
            segments.put(newFileName, openTable(newFileName));
            tablesNames.put(newFileName, new AtomicReference<>(newFileName));
            tablesLocks.put(newFileName, new ReentrantReadWriteLock());
            storage.setFlushStatus(false);
            newName.set(newFileName);
        }
    }
    public void compact(Path storagePath) throws IOException {
        List<String> tables;
        MergeIterator firstIter;
        MergeIterator secondIter;
        MergeIterator thirdIter;
        compactWriteLock.lock();
        try {
            isCompacting = true;
            tables = new ArrayList<>(segments.keySet());
            firstIter = getRange(null, null, null);
            secondIter = getRange(null, null, null);
            thirdIter = getRange(null, null, null);

        } finally {
            compactWriteLock.unlock();
        }
        String newFileName = "compaction.tmp";
        Path compactionTmpFile = storagePath.resolve(newFileName);

        long dataSize = 0;
        long count = 0;
        while (firstIter.hasNext()) {
            Entry<MemorySegment> entry = firstIter.next();
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(
                        compactionTmpFile,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize + dataSize,
                    writeArena
            );

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            while (secondIter.hasNext()) {
                Entry<MemorySegment> entry = secondIter.next();
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                dataOffset += entry.key().byteSize();
                indexOffset += Long.BYTES;

                MemorySegment value = entry.value();
                if (value == null) {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(dataOffset));
                } else {
                    fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    dataOffset += value.byteSize();
                }
                indexOffset += Long.BYTES;
            }

            // data:
            // |key0|value0|key1|value1|...
            dataOffset = indexSize;
            while (thirdIter.hasNext()) {
                Entry<MemorySegment> entry = thirdIter.next();
                MemorySegment key = entry.key();
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();
                if (value != null) {
                    MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
            }
        }

        Files.move(
                compactionTmpFile,
                compactionFile(storagePath),
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );

        clearAndChange(storagePath, tables);
    }

    private void clearAndChange(Path storagePath, List<String> deletingTables) throws IOException {
        Path compactionFile = compactionFile(storagePath);
        List<Lock> locks = new ArrayList<>(deletingTables.size());
        try{
            String compactedTableName = SSTABLE_PREFIX + "0";
            for (var name : deletingTables) {
                var lock = tablesLocks.get(name).writeLock();
                lock.lock();
                locks.add(lock);
                tablesNames.get(name).setRelease(compactedTableName);
                tablesNames.remove(name);
                tablesLocks.remove(name);
                segments.remove(name);
            }
            for (var name : deletingTables) {
             var p = storagePath.resolve(name);
             Files.delete(p);
            }

            Path indexTmp = storagePath.resolve("index.tmp");
            Path indexFile = storagePath.resolve("index.idx");
            boolean noData = Files.size(compactionFile) == 0;

            compactWriteLock.lock();
            try {
                List<String> indexList = new ArrayList<>(tablesOverCompacting.size() + 1);
                indexList.add(compactedTableName);
                indexList.addAll(tablesOverCompacting);
                Files.deleteIfExists(indexFile);
                Files.deleteIfExists(indexTmp);

                Files.write(
                        indexTmp,
                        indexList,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.TRUNCATE_EXISTING
                );
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
                if (noData) {
                    Files.delete(compactionFile);
                } else {
                    Files.move(compactionFile, storagePath.resolve(compactedTableName), StandardCopyOption.ATOMIC_MOVE);
                }
                segments.put(compactedTableName, openTable(compactedTableName));
                tablesNames.put(compactedTableName, new AtomicReference<>(compactedTableName));
                tablesLocks.put(compactedTableName, new ReentrantReadWriteLock());
                tablesOverCompacting.clear();
                isCompacting = false;
            } finally {
                compactWriteLock.unlock();
            }

        } finally {
            for (var lock : locks) {
                lock.unlock();
            }
        }
    }

    public MemorySegment openTable(String tableName) throws IOException {
        Path file = storagePath.resolve(tableName);
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            return fileChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Files.size(file),
                    arena
            );
        }
    }
    public AtomicReference<String> getAtomicForTable(String name) {
        return tablesNames.get(name);
    }
    public Lock getReadLockForTable(String name) {
        return tablesLocks.get(name).readLock();
    }
    private static void finalizeCompaction(Path storagePath) throws IOException {
        Path compactionFile = compactionFile(storagePath);
        try (Stream<Path> stream = Files.find(storagePath, 1, (path, attrs) -> path.getFileName().toString().startsWith(SSTABLE_PREFIX))) {
            stream.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        Path indexTmp = storagePath.resolve("index.tmp");
        Path indexFile = storagePath.resolve("index.idx");

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        boolean noData = Files.size(compactionFile) == 0;

        Files.write(
                indexTmp,
                noData ? Collections.emptyList() : Collections.singleton(SSTABLE_PREFIX + "0"),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        if (noData) {
            Files.delete(compactionFile);
        } else {
            Files.move(compactionFile, storagePath.resolve(SSTABLE_PREFIX + "0"), StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static Path compactionFile(Path storagePath) {
        return storagePath.resolve("compaction");
    }


    public static ConcurrentHashMap<String, MemorySegment> loadOrRecover(Path storagePath, Arena arena) throws IOException {
        if (Files.exists(compactionFile(storagePath))) {
            finalizeCompaction(storagePath);
        }

        Path indexTmp = storagePath.resolve("index.tmp");
        Path indexFile = storagePath.resolve("index.idx");

        if (!Files.exists(indexFile)) {
            if (Files.exists(indexTmp)) {
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                Files.createFile(indexFile);
            }
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        ConcurrentHashMap<String, MemorySegment> result = new ConcurrentHashMap<>();
        for (String fileName : existedFiles) {
            Path file = storagePath.resolve(fileName);
            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0,
                        Files.size(file),
                        arena
                );
                result.put(fileName, fileSegment);
            }
        }

        return result;
    }

    public static long indexOf(MemorySegment segment, MemorySegment key) {
        long recordsCount = recordsCount(segment);

        long left = 0;
        long right = recordsCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;

            long startOfKey = startOfKey(segment, mid);
            long endOfKey = endOfKey(segment, mid);
            long mismatch = MemorySegment.mismatch(segment, startOfKey, endOfKey, key, 0, key.byteSize());
            if (mismatch == -1) {
                return mid;
            }

            if (mismatch == key.byteSize()) {
                right = mid - 1;
                continue;
            }

            if (mismatch == endOfKey - startOfKey) {
                left = mid + 1;
                continue;
            }

            int b1 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, startOfKey + mismatch));
            int b2 = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, mismatch));
            if (b1 > b2) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return tombstone(left);
    }

    public static long recordsCount(MemorySegment segment) {
        long indexSize = indexSize(segment);
        return indexSize / Long.BYTES / 2;
    }

    private static long indexSize(MemorySegment segment) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public SegmentIterInterface iterator(String name, MemorySegment from, MemorySegment to) {
        return new SegmentIterator(from, to, name,
                tablesNames.get(name), tablesLocks.get(name).readLock(), this);
    }
    public MemorySegment getSegment(String name) {
        return segments.get(name);
    }
    public static long startOfKey(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES);
    }

    public static long endOfKey(MemorySegment segment, long recordIndex) {
        return normalizedStartOfValue(segment, recordIndex);
    }

    private static long normalizedStartOfValue(MemorySegment segment, long recordIndex) {
        return normalize(startOfValue(segment, recordIndex));
    }

    public static long startOfValue(MemorySegment segment, long recordIndex) {
        return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * 2 * Long.BYTES + Long.BYTES);
    }

    public static long endOfValue(MemorySegment segment, long recordIndex, long recordsCount) {
        if (recordIndex < recordsCount - 1) {
            return startOfKey(segment, recordIndex + 1);
        }
        return segment.byteSize();
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    public static long normalize(long value) {
        return value & ~(1L << 63);
    }

}