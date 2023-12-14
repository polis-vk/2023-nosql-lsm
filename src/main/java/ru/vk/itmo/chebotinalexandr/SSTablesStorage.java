package ru.vk.itmo.chebotinalexandr;

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
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.vk.itmo.chebotinalexandr.SSTableUtils.BLOOM_FILTER_BIT_SIZE_OFFSET;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.BLOOM_FILTER_HASH_FUNCTIONS_OFFSET;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.BLOOM_FILTER_LENGTH_OFFSET;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.COMPACTION_NOT_FINISHED_TAG;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.ENTRIES_SIZE_OFFSET;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.OLDEST_SS_TABLE_INDEX;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.TOMBSTONE;
import static ru.vk.itmo.chebotinalexandr.SSTableUtils.entryByteSize;

public class SSTablesStorage {
    private static final String SSTABLE_NAME = "sstable_";
    private static final String SSTABLE_EXTENSION = ".dat";
    private final Path basePath;
    public static final int HASH_FUNCTIONS_NUM = 2;

    public SSTablesStorage(Path basePath) {
        this.basePath = basePath;
    }

    public static List<MemorySegment> loadOrRecover(Path basePath, Arena arena) {
        List<MemorySegment> sstables = new ArrayList<>();

        if (compactionTmpFileExists(basePath)) {
            restoreCompaction(basePath, arena);
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

        return sstables;
    }

    private static boolean compactionTmpFileExists(Path basePath) {
        Path pathTmp = basePath.resolve(SSTABLE_NAME + ".tmp");
        return Files.exists(pathTmp);
    }

    private static void restoreCompaction(Path basePath, Arena arena) {
        Path pathTmp = basePath.resolve(SSTABLE_NAME + ".tmp");

        try (FileChannel channel = FileChannel.open(pathTmp, StandardOpenOption.READ)) {
            MemorySegment tmpSstable = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);

            long tag = tmpSstable.get(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET);
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

    private static int parsePriority(Path path) {
        String fileName = path.getFileName().toString();
        return Integer.parseInt(fileName.substring(fileName.indexOf('_') + 1, fileName.indexOf('.')));
    }

    private static Comparator<Map.Entry<Path, Integer>> priorityComparator() {
        return Comparator.comparingInt(Map.Entry::getValue);
    }

    public static long find(MemorySegment readSegment, MemorySegment key) {
        return SSTableUtils.binarySearch(readSegment, key);

    }

    public static Iterator<Entry<MemorySegment>> iteratorsAll(List<MemorySegment> segments, MemorySegment from, MemorySegment to) {
        List<PeekingIterator<Entry<MemorySegment>>> result = new ArrayList<>();

        int priority = 1;
        for (MemorySegment sstable : segments) {
            result.add(new PeekingIteratorImpl<>(iteratorOf(sstable, from, to), priority));
            priority++;
        }
        return MergeIterator.merge(result, NotOnlyInMemoryDao::entryComparator);
    }

    public static Iterator<Entry<MemorySegment>> iteratorOf(MemorySegment sstable, MemorySegment from, MemorySegment to) {
        long keyIndexFrom;
        long keyIndexTo;

        if (from == null && to == null) {
            keyIndexFrom = 0;
            keyIndexTo = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET);
        } else if (from == null) {
            keyIndexFrom = 0;
            keyIndexTo = find(sstable, to);
        } else if (to == null) {
            keyIndexFrom = find(sstable, from);
            keyIndexTo = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET);
        } else {
            keyIndexFrom = find(sstable, from);
            keyIndexTo = find(sstable, to);
        }

        final long bloomFilterLength = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_LENGTH_OFFSET);
        final long keyOffset = 4L * Long.BYTES + bloomFilterLength * Long.BYTES;

        return new SSTableIterator(sstable, keyIndexFrom, keyIndexTo, keyOffset);
    }

    // |bf length|bf bit size|hash_functions_count|entries length|key1 offset|
    // |key2 offset| ... |key_n offset|key1Size|key1|value1Size|value1| ...
    public MemorySegment write(Collection<Entry<MemorySegment>> dataToFlush) throws IOException {
        long size = 0;

        for (Entry<MemorySegment> entry : dataToFlush) {
            size += entryByteSize(entry);
        }

        long bloomFilterLength = BloomFilter.divide(dataToFlush.size(), Long.SIZE);

        size += 2L * Long.BYTES * dataToFlush.size();
        size += 4L * Long.BYTES + Long.BYTES * dataToFlush.size(); //for metadata (header + key offsets)
        size += Long.BYTES * bloomFilterLength; //for bloom filter

        MemorySegment memorySegment;
        try (Arena arenaForSave = Arena.ofShared()) {
            memorySegment = writeMappedSegment(size, arenaForSave);

            //Writing sstable header
            long headerOffset = 0;

            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_LENGTH_OFFSET, bloomFilterLength);
            headerOffset += Long.BYTES;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_BIT_SIZE_OFFSET, (long) bloomFilterLength * Long.SIZE);
            headerOffset += Long.BYTES;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_HASH_FUNCTIONS_OFFSET, HASH_FUNCTIONS_NUM);
            headerOffset += Long.BYTES;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET, dataToFlush.size());
            headerOffset += Long.BYTES;
            //---------

            //Writing bloom filter + memory entries
            long bloomFilterOffset = headerOffset;
            final long keyOffset = bloomFilterOffset + bloomFilterLength * Long.BYTES;
            long offset = keyOffset + Long.BYTES * dataToFlush.size();

            long i = 0;
            for (Entry<MemorySegment> entry : dataToFlush) {
                BloomFilter.addToSstable(entry.key(), memorySegment, HASH_FUNCTIONS_NUM, (long) bloomFilterLength * Long.SIZE);
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset + i * Long.BYTES, offset);
                offset = writeEntry(entry, memorySegment, offset);
                i++;
            }
            //---------

        }

        return memorySegment;
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

    public MemorySegment compact(Iterator<Entry<MemorySegment>> iterator,
                        long sizeForCompaction, long entryCount, long bfLength) throws IOException {
        Path path = basePath.resolve(SSTABLE_NAME + ".tmp");

        MemorySegment memorySegment;
        try (Arena arenaForCompact = Arena.ofShared()) {
            try (FileChannel channel = FileChannel.open(path,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE)) {

                memorySegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, sizeForCompaction,
                        arenaForCompact);
            }

            //Writing sstable header
            long headerOffset = 0;

            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_LENGTH_OFFSET, bfLength);
            headerOffset += Long.BYTES;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_BIT_SIZE_OFFSET, bfLength * Long.SIZE);
            headerOffset += Long.BYTES;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, BLOOM_FILTER_HASH_FUNCTIONS_OFFSET, HASH_FUNCTIONS_NUM);
            headerOffset += Long.BYTES;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET, COMPACTION_NOT_FINISHED_TAG);
            headerOffset += Long.BYTES;
            //---------

            //Writing new Bloom filter and entries
            long bloomFilterOffset = headerOffset;
            final long keyOffset = bloomFilterOffset + bfLength * Long.BYTES;
            long offset = keyOffset + Long.BYTES * entryCount;

            long index = 0;
            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();

                BloomFilter.addToSstable(entry.key(), memorySegment, HASH_FUNCTIONS_NUM, bfLength * Long.SIZE);
                memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset + index * Long.BYTES, offset);
                offset = writeEntry(entry, memorySegment, offset);
                index++;
            }

            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, ENTRIES_SIZE_OFFSET, entryCount);
        }


        deleteOldSSTables(basePath);
        Files.move(path, path.resolveSibling(SSTABLE_NAME + OLDEST_SS_TABLE_INDEX + SSTABLE_EXTENSION),
                StandardCopyOption.ATOMIC_MOVE);

        return memorySegment;
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
}
