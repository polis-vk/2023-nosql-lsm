package ru.vk.itmo.grunskiialexey;

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
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemorySegmentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = (o1, o2) -> {
        long firstMismatch = o1.mismatch(o2);
        if (firstMismatch == -1) {
            return 0;
        }
        if (firstMismatch == o1.byteSize()) {
            return -1;
        }
        if (firstMismatch == o2.byteSize()) {
            return 1;
        }

        byte byte1 = o1.get(ValueLayout.JAVA_BYTE, firstMismatch);
        byte byte2 = o2.get(ValueLayout.JAVA_BYTE, firstMismatch);
        return Byte.compare(byte1, byte2);
    };

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> data = new ConcurrentSkipListMap<>(comparator);
    private final Path filePath;
    private final Arena arena;
    private final MemorySegment page;

    public MemorySegmentDao(Config config) throws IOException {
        this.filePath = Paths.get(config.basePath().toString(), "file.db");
        arena = Arena.ofShared();

        long size;
        try {
            size = Files.size(filePath);
        } catch (NoSuchFileException e) {
            page = MemorySegment.NULL;
            return;
        }

        MemorySegment currentPage = null;
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            currentPage = channel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        } catch (IOException e) {
            arena.close();
        } finally {
            page = currentPage;
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return data.values().iterator();
        } else if (from == null) {
            return data.headMap(to).values().iterator();
        } else if (to == null) {
            return data.tailMap(from).values().iterator();
        } else {
            return data.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        if (data.containsKey(key)) {
            return data.get(key);
        }
        if (page.equals(MemorySegment.NULL)) {
            return null;
        }

        long offset = 0;
        while (offset < page.byteSize()) {
            int keyLength = page.get(ValueLayout.JAVA_INT, offset);
            offset += 4;
            MemorySegment resultKey = MemorySegment.ofArray(new byte[keyLength]);
            MemorySegment.copy(page, ValueLayout.JAVA_BYTE, offset, resultKey, ValueLayout.JAVA_BYTE, 0, keyLength);
            offset += correctAlignedSize(keyLength);

            int valueLength = page.get(ValueLayout.JAVA_INT, offset);
            offset += 4;
            MemorySegment resultValue = MemorySegment.ofArray(new byte[valueLength]);
            MemorySegment.copy(page, ValueLayout.JAVA_BYTE, offset, resultValue, ValueLayout.JAVA_BYTE, 0, valueLength);
            offset += correctAlignedSize(valueLength);

            if (resultKey.mismatch(key) == -1) {
                return new BaseEntry<>(
                        resultKey,
                        resultValue
                );
            }
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            data.remove(entry.key());
        } else {
            data.put(entry.key(), entry);
        }
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        arena.close();

        try (
                FileChannel channel = FileChannel.open(
                        filePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            long allSize = data.values().stream().mapToLong(entry ->
                    correctAlignedSize(entry.key().byteSize()) + correctAlignedSize(entry.value().byteSize()) + 2 * 4
            ).sum();
            MemorySegment writePage = channel.map(FileChannel.MapMode.READ_WRITE, 0, allSize, writeArena);

            long offset = 0;
            for (Entry<MemorySegment> entry : data.values()) {
                long keyLength = entry.key().byteSize();
                writePage.set(ValueLayout.JAVA_INT, offset, (int) keyLength);
                offset += 4;
                MemorySegment.copy(
                        entry.key(), ValueLayout.JAVA_BYTE, 0,
                        writePage, ValueLayout.JAVA_BYTE, offset, keyLength
                );
                offset += correctAlignedSize(keyLength);

                long valueLength = entry.value().byteSize();
                writePage.set(ValueLayout.JAVA_INT, offset, (int) valueLength);
                offset += 4;
                MemorySegment.copy(
                        entry.value(), ValueLayout.JAVA_BYTE, 0,
                        writePage, ValueLayout.JAVA_BYTE, offset, valueLength
                );
                offset += correctAlignedSize(valueLength);
            }
        }
    }

    private long correctAlignedSize(long offset) {
        return offset % 4 == 0 ? offset : offset + 4 - (offset % 4);
    }
}
