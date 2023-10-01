package ru.vk.itmo.abramovilya;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.Math.min;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final int CHUNK_SIZE = 1024;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map =
            new ConcurrentSkipListMap<>(InMemoryDao::compareMemorySegments);
    private final Path storagePath;

    private final Arena arena = Arena.ofConfined();

    private long offset = 0;

    public InMemoryDao() {
        storagePath = null;
    }

    public InMemoryDao(Config config) {
        storagePath = config.basePath().resolve("storage");
        if (!Files.exists(storagePath)) {
            try {
                Files.createFile(storagePath);
            } catch (IOException _) {
            }
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> subMap;
        if (from == null && to == null) {
            subMap = map;
        } else if (from == null) {
            subMap = map.headMap(to);
        } else if (to == null) {
            subMap = map.tailMap(from);
        } else {
            subMap = map.subMap(from, to);
        }
        return subMap.values().iterator();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        map.put(entry.key(), entry);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var value = map.get(key);
        if (value != null) {
            return value;
        }
        return seekForValueInFile(key);
    }

    private Entry<MemorySegment> seekForValueInFile(MemorySegment key) {
        if (storagePath == null || !Files.exists(storagePath)) {
            return null;
        }
        offset = 0;
        MemorySegment lastMemorySegment = null;
        try (FileChannel fc = FileChannel.open(storagePath, READ)) {
            MemorySegment mapped = fc.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(storagePath), arena);
            while (offset < Files.size(storagePath)) {
                MemorySegment keySegment = getMemorySegment(mapped);
                if (compareMemorySegments(key, keySegment) == 0) {
                    lastMemorySegment = getMemorySegment(mapped);
                }
            }
            if (lastMemorySegment != null) {
                return new BaseEntry<>(key, lastMemorySegment);
            }
            return null;
        } catch (IOException e) {
            throw new RuntimeException("Can't open storage file " + STR. "\{ e.getMessage() }" );
        }
    }

    private MemorySegment getMemorySegment(MemorySegment mappedMemory) throws IOException {
        long size = mappedMemory.get(ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN), offset);
        offset += Long.BYTES;
        MemorySegment memorySegment = mappedMemory.asSlice(offset, size);
        offset += size;
        return memorySegment;

    }

    private static int compareMemorySegments(MemorySegment segment1, MemorySegment segment2) {
        long offset = segment1.mismatch(segment2);
        if (offset == -1) {
            return 0;
        } else if (offset == segment1.byteSize()) {
            return -1;
        } else if (offset == segment2.byteSize()) {
            return 1;
        }
        return Byte.compare(
                segment1.get(ValueLayout.JAVA_BYTE, offset),
                segment2.get(ValueLayout.JAVA_BYTE, offset)
        );
    }

    @Override
    public void flush() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
        arena.close();
        writeMapIntoFile();
    }

    private void writeMapIntoFile() throws IOException {
        try (WritableByteChannel channel = Files.newByteChannel(storagePath, WRITE)) {
            for (var entry : map.values()) {
                writeMemorySegment(entry.key(), channel);
                writeMemorySegment(entry.value(), channel);
            }
        }
    }

    // Every memorySegment in file has the following structure:
    // 8 bytes - size, <size> bytes - value
    private static void writeMemorySegment(MemorySegment memorySegment, WritableByteChannel channel) throws IOException {
        long msSize = memorySegment.byteSize();
        ByteBuffer sizeBuffer = ByteBuffer.allocate(Long.BYTES);
        sizeBuffer.putLong(msSize);
        channel.write(sizeBuffer.flip());
//        channel.write(memorySegment.asByteBuffer());

        long offset = 0;
        while (offset < msSize) {
            var chunk = memorySegment.asSlice(offset, min(CHUNK_SIZE, msSize - offset)).asByteBuffer();
            offset += CHUNK_SIZE;
            channel.write(chunk);
        }
    }
}
