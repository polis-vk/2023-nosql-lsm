package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SSTablesStorage {
    private static final String SSTABLE_NAME = "sstable_";
    private static final String SSTABLE_EXTENSION = ".dat";
    private static final long TOMBSTONE = -1;
    private static final long COMPACTION_NOT_FINISHED_TAG = -1;
    private final Path basePath;
    public static final long OFFSET_FOR_SIZE = 0;
    private static final long OLDEST_SS_TABLE_INDEX = 0;
    private final List<MemorySegment> sstables;
    private final Arena arena;

    public SSTablesStorage(Config config) {
        basePath = config.basePath();

        arena = Arena.ofShared();
        sstables = new ArrayList<>();

        if (compactionTmpFileExists()) {
            restoreCompaction();
        }

        try (Stream<Path> stream = Files.list(basePath)) {
            stream
                    .filter(path -> path.toString().endsWith(SSTABLE_EXTENSION))
                    .map(path -> new AbstractMap.SimpleEntry<>(path, parsePriority(path)))
                    .sorted(priorityComparator().reversed())
                    .forEach(entry -> {
                        try (FileChannel channel = FileChannel.open(entry.getKey(), StandardOpenOption.READ)) {
                            MemorySegment readSegment = channel.map(
                                    FileChannel.MapMode.READ_ONLY,
                                    0,
                                    channel.size(),
                                    arena);

                            sstables.add(readSegment);
                        } catch (FileNotFoundException | NoSuchFileException e) {
                            arena.close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (NoSuchFileException e) {
            arena.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean compactionTmpFileExists() {
        Path pathTmp = basePath.resolve(SSTABLE_NAME + ".tmp");
        return Files.exists(pathTmp);
    }

    private void restoreCompaction() {
        Path pathTmp = basePath.resolve(SSTABLE_NAME + ".tmp");

        try (FileChannel channel = FileChannel.open(pathTmp, StandardOpenOption.READ)) {
            MemorySegment tmpSstable = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);

            long tag = tmpSstable.get(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE);
            if (tag == COMPACTION_NOT_FINISHED_TAG) {
                Files.delete(pathTmp);
            } else {
                deleteOldSSTables(basePath);
                Files.move(pathTmp, pathTmp.resolveSibling(SSTABLE_NAME + OLDEST_SS_TABLE_INDEX + SSTABLE_EXTENSION),
                        StandardCopyOption.ATOMIC_MOVE);
            }

        } catch (FileNotFoundException | NoSuchFileException e) {
            arena.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private int parsePriority(Path path) {
        String fileName = path.getFileName().toString();
        return Integer.parseInt(fileName.substring(fileName.indexOf('_') + 1, fileName.indexOf('.')));
    }

    private Comparator<Map.Entry<Path, Integer>> priorityComparator() {
        return Comparator.comparingInt(Map.Entry::getValue);
    }

    public long find(MemorySegment readSegment, MemorySegment key) {
        return SSTableUtils.binarySearch(readSegment, key);

    }

    //Merge iterator from all sstables in sstable storage
    public Iterator<Entry<MemorySegment>> iteratorsAll(MemorySegment from, MemorySegment to) {
        List<PeekingIterator<Entry<MemorySegment>>> result = new ArrayList<>();

        int priority = 1;
        for (MemorySegment sstable : sstables) {
            result.add(new PeekingIteratorImpl<>(iteratorOf(sstable, from, to), priority));
            priority++;
        }
        return MergeIterator.merge(result, NotOnlyInMemoryDao::entryComparator);
    }

    public Iterator<Entry<MemorySegment>> iteratorOf(MemorySegment sstable, MemorySegment from, MemorySegment to) {
        long keyIndexFrom;
        long keyIndexTo;

        if (from == null && to == null) {
            keyIndexFrom = 0;
            keyIndexTo = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE);
        } else if (from == null) {
            keyIndexFrom = 0;
            keyIndexTo = find(sstable, to);
        } else if (to == null) {
            keyIndexFrom = find(sstable, from);
            keyIndexTo = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE);
        } else {
            keyIndexFrom = find(sstable, from);
            keyIndexTo = find(sstable, to);
        }

        return new SSTableIterator(sstable, keyIndexFrom, keyIndexTo);
    }

    public void write(SortedMap<MemorySegment, Entry<MemorySegment>> dataToFlush) throws IOException {
        long size = 0;

        for (Entry<MemorySegment> entry : dataToFlush.values()) {
            size += entryByteSize(entry);
        }
        size += 2L * Long.BYTES * dataToFlush.size();
        size += Long.BYTES + Long.BYTES * dataToFlush.size(); //for metadata (header + key offsets)

        MemorySegment memorySegment;
        try (Arena arenaForSave = Arena.ofConfined()) {
            memorySegment = writeMappedSegment(size, arenaForSave);

            long offset = 0;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE, dataToFlush.size());
            offset += Long.BYTES + Long.BYTES * dataToFlush.size();

            long i = 0;
            for (Entry<MemorySegment> entry : dataToFlush.values()) {
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + i * Byte.SIZE, offset);
                offset = writeEntry(entry, memorySegment, offset);
                i++;
            }

        }
        arena.close();
    }

    private long writeEntry(Entry<MemorySegment> entry, MemorySegment dst, long offset) {
        long newOffset = writeSegment(entry.key(), dst, offset);

        if (entry.value() == null) {
            dst.set(ValueLayout.JAVA_LONG_UNALIGNED, newOffset, TOMBSTONE);
            newOffset += Long.BYTES;
        } else {
            newOffset = writeSegment(entry.value(), dst, newOffset);
        }

        return newOffset;
    }

    private long writeSegment(MemorySegment src, MemorySegment dst, long offset) {
        long size = src.byteSize();
        long newOffset = offset;

        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, newOffset, size);
        newOffset += Long.BYTES;
        MemorySegment.copy(src, 0, dst, newOffset, size);
        newOffset += size;

        return newOffset;
    }

    private static List<Path> getPaths(Path basePath) throws IOException {
        try (Stream<Path> s = Files.list(basePath)) {
            return s.filter(path -> path.toString().endsWith(SSTABLE_EXTENSION)).collect(Collectors.toList());
        }
    }

    private static void deleteOldSSTables(Path basePath) throws IOException {
        try (Stream<Path> stream = Files.list(basePath)) {
            stream
                    .filter(path -> path.toString().endsWith(SSTABLE_EXTENSION))
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
    }

    public void compact(Iterator<Entry<MemorySegment>> iterator,
                        long sizeForCompaction, long entryCount) throws IOException {
        Path path = basePath.resolve(SSTABLE_NAME + ".tmp");

        MemorySegment memorySegment;
        try (Arena arenaForCompact = Arena.ofConfined()) {
            try (FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE)) {

                memorySegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, sizeForCompaction,
                        arenaForCompact);
            }

            long offset = 0;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE, COMPACTION_NOT_FINISHED_TAG);
            offset += Long.BYTES; //header
            offset += Long.BYTES * entryCount; //key offsets

            long i = 0;
            while (iterator.hasNext()) {
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + i * Byte.SIZE, offset);
                offset = writeEntry(iterator.next(), memorySegment, offset);
                i++;
            }

            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE, entryCount); //our header
        }

        deleteOldSSTables(basePath);
        //renaming with Files more reliable
        Files.move(path, path.resolveSibling(SSTABLE_NAME + OLDEST_SS_TABLE_INDEX + SSTABLE_EXTENSION),
                StandardCopyOption.ATOMIC_MOVE);
    }

    private MemorySegment writeMappedSegment(long size, Arena arena) throws IOException {
        int count = getPaths(basePath).size() + 1;
        Path path = basePath.resolve(SSTABLE_NAME + count + SSTABLE_EXTENSION);

        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE)) {

            return channel.map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
        }
    }

    public static long entryByteSize(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return entry.key().byteSize();
        }

        return entry.key().byteSize() + entry.value().byteSize();
    }
}
