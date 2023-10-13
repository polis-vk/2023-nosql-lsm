package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.BaseEntry;
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
import java.util.SortedMap;

public class SSTable {
    private static final String SSTABLE_NAME = "sstable";
    private final Path path;
    private static final long OFFSET_FOR_SIZE = 0;
    private MemorySegment readSegment;

    public SSTable(Config config) {
        path = config.basePath().resolve(SSTABLE_NAME);

        Arena arena = Arena.ofConfined();
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            readSegment = channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    channel.size(),
                    arena);
        } catch (FileNotFoundException | NoSuchFileException e) {
            arena.close();
            readSegment = null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (Files.notExists(path)) {
            return null;
        }

        long low = -1;
        long high = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE);

        while (low < high - 1) {
            long mid = (high - low) / 2 + low;

            long offset = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + mid * Byte.SIZE);

            long keySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment currKey = readSegment.asSlice(offset, keySize);
            offset += currKey.byteSize();

            int compare = InMemoryDao.comparator(currKey, key);

            if (compare == 0) {
                long valueSize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                return new BaseEntry<>(key, readSegment.asSlice(offset, valueSize));
            } else if (compare > 0) {
                high = mid;
            } else {
                low = mid;
            }
        }

        return null;
    }

    public void write(SortedMap<MemorySegment, Entry<MemorySegment>> dataToFlush) throws IOException {
        long size = 0;

        for (Entry<MemorySegment> entry : dataToFlush.values()) {
            size += entryByteSize(entry);
        }
        size += 2L * Long.BYTES * dataToFlush.size();
        size += Long.BYTES + Long.BYTES * dataToFlush.size();

        MemorySegment memorySegment;
        try (Arena arena = Arena.ofConfined()) {
            memorySegment = writeMappedSegment(size, arena);

            long offset = 0;
            memorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, OFFSET_FOR_SIZE, dataToFlush.size());
            offset += Long.BYTES + Long.BYTES * dataToFlush.size();

            long i = 0;
            for (Entry<MemorySegment> entry : dataToFlush.values()) {
                memorySegment.set(
                        ValueLayout.JAVA_LONG_UNALIGNED,
                        Long.BYTES + i * Byte.SIZE, offset
                );
                offset = writeSegment(entry.key(), memorySegment, offset);
                offset = writeSegment(entry.value(), memorySegment, offset);
                i++;
            }
        }

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

    private MemorySegment writeMappedSegment(long size, Arena arena) throws IOException {
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
        return entry.key().byteSize() + entry.value().byteSize();
    }
}

