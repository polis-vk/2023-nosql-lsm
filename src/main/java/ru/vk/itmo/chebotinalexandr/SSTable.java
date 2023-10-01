package ru.vk.itmo.chebotinalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.SortedMap;

public class SSTable {
    public static final String SSTABLE_NAME = "sstable";
    private final Path path;

    public SSTable(Config config) {
        path = config.basePath().resolve(SSTABLE_NAME);

    }

    public Entry<MemorySegment> get(MemorySegment key) {

        if (Files.notExists(path)) {
            return null;
        }

        MemorySegment readSegment = readMappedSegment();

        long offset = 0;
        while (offset < readSegment.byteSize()) {
            long currKeySize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long valueSize = readSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            MemorySegment currKey = readSegment.asSlice(offset, currKeySize);
            offset += currKeySize;

            if (InMemoryDao.comparator(key, currKey) == 0) {
                MemorySegment value = readSegment.asSlice(offset, valueSize);
                return new BaseEntry<>(key, value);
            }

            offset += valueSize;
        }

        return null;
    }

    public void write(SortedMap<MemorySegment, Entry<MemorySegment>> dataToFlush) throws IOException {
        long size = 0;

        for (Entry<MemorySegment> entry : dataToFlush.values()) {
            if (entry == null) continue;
            size += entryByteSize(entry);
        }
        size += 2L * Long.BYTES * dataToFlush.size();

        MemorySegment writeSegment = writeMappedSegment(size);

        long offset = 0;
        for (Entry<MemorySegment> entry : dataToFlush.values()) {

            long keySize = entry.key().byteSize();
            long valueSize = entry.value().byteSize();

            writeSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, keySize);
            offset += Long.BYTES;
            writeSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, valueSize);
            offset += Long.BYTES;

            MemorySegment.copy(entry.key(), 0, writeSegment, offset, keySize);
            offset += keySize;
            MemorySegment.copy(entry.value(), 0, writeSegment, offset, valueSize);
            offset += valueSize;
        }
    }

    private MemorySegment readMappedSegment() {

        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return channel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    channel.size(),
                    Arena.ofConfined());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private MemorySegment writeMappedSegment(long size) throws IOException {

        try (FileChannel channel = FileChannel.open(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            return channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    size,
                    Arena.ofConfined());
        }
    }

    private long entryByteSize(Entry<MemorySegment> entry) {
        return entry.key().byteSize() + entry.value().byteSize();
    }
}
