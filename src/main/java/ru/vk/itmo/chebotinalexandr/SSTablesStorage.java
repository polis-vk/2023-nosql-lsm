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
    private final Path basePath;
    private static final long OFFSET_FOR_SIZE = 0;
    private final List<MemorySegment> sstables;
    private final Arena arena;

    public SSTablesStorage(Config config) {
        basePath = config.basePath();

        arena = Arena.ofConfined();
        sstables = new ArrayList<>();
        try (Stream<Path> stream = Files.list(basePath)) {
            stream
                    .filter(path -> path.toString().endsWith(SSTABLE_EXTENSION))
                    .map(path -> new AbstractMap.SimpleEntry<>(path, parsePriority(path)))
                    .sorted(priorityComparator().reversed())
                    .forEach(entry -> {
                        try (var channel = FileChannel.open(entry.getKey(), StandardOpenOption.READ)) {
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

    private int parsePriority(Path path) {
        String fileName = path.getFileName().toString();
        return Integer.parseInt(fileName.substring(fileName.indexOf('_') + 1, fileName.indexOf('.')));
    }

    private Comparator<Map.Entry<Path, Integer>> priorityComparator() {
        return Comparator.comparingInt(Map.Entry::getValue);
    }

    //Returns index
    public long binarySearch(MemorySegment readSegment, MemorySegment key) {
        long low = -1;
        long high = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE);

        while (low < high - 1) {
            long mid = (high - low) / 2 + low;

            long offset = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + mid * Byte.SIZE);

            long keySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment midKey = readSegment.asSlice(offset, keySize);

            int compare = NotOnlyInMemoryDao.comparator(midKey, key);

            if (compare == 0) {
                return mid;
            }
            if (compare > 0) {
                high = mid;
            } else {
                low = mid;
            }
        }

        return low + 1;
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

    //Iterator from sstable
    public Iterator<Entry<MemorySegment>> iteratorOf(MemorySegment sstable, MemorySegment from, MemorySegment to) {
        long keyIndexFrom;
        long keyIndexTo;

        if (from == null && to == null) {
            keyIndexFrom = 0;
            keyIndexTo = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE);
        } else if (from == null) {
            keyIndexFrom = 0;
            keyIndexTo = binarySearch(sstable, to);
        } else if (to == null) {
            keyIndexFrom = binarySearch(sstable, from);
            keyIndexTo = sstable.get(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE);
        } else {
            keyIndexFrom = binarySearch(sstable, from);
            keyIndexTo = binarySearch(sstable, to);
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
                memorySegment.set(
                        ValueLayout.JAVA_LONG_UNALIGNED,
                        Long.BYTES + i * Byte.SIZE, offset
                );

                offset = writeEntry(entry, memorySegment, offset);
                i++;
            }
        }
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

    private MemorySegment writeMappedSegment(long size, Arena arena) throws IOException {
        int count = getPaths(basePath).size() + 1;
        var path = basePath.resolve(SSTABLE_NAME + count + SSTABLE_EXTENSION);

        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            return channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    size,
                    arena);
        }
    }

    private long entryByteSize(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return entry.key().byteSize();
        }

        return entry.key().byteSize() + entry.value().byteSize();
    }
}
