package ru.vk.itmo.grunskiialexey;

import ru.vk.itmo.Entry;
import ru.vk.itmo.grunskiialexey.model.ActualFilesInterval;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static ru.vk.itmo.grunskiialexey.DiskStorage.changeActualFilesInterval;
import static ru.vk.itmo.grunskiialexey.DiskStorage.endOfKey;
import static ru.vk.itmo.grunskiialexey.DiskStorage.tombstone;

public class InMemoryQuerySystem {
    private static final String NAME_TMP_INDEX_FILE = "index.tmp";
    private static final String NAME_INDEX_FILE = "index.idx";
    private final List<NavigableMap<MemorySegment, Entry<MemorySegment>>> storages = new ArrayList<>(2);
    private final Path flushPath;
    private final AtomicLong lastFileNumber;
    private final long flushThresholdBytes;
    private final AtomicBoolean isWorking;
    private final AtomicLong currentByteSize;
    private final DiskStorage diskStorage;
    private final Arena arena;

    public InMemoryQuerySystem(
            Path flushPath,
            long flushThresholdBytes,
            Comparator<MemorySegment> comparator,
            AtomicLong lastFileNumber,
            DiskStorage diskStorage,
            Arena arena
    ) {
        storages.addAll(List.of(new ConcurrentSkipListMap<>(comparator), new ConcurrentSkipListMap<>(comparator)));

        this.flushPath = flushPath;
        this.lastFileNumber = lastFileNumber;
        this.flushThresholdBytes = flushThresholdBytes;
        this.arena = arena;
        this.isWorking = new AtomicBoolean();
        this.currentByteSize = new AtomicLong();
        this.diskStorage = diskStorage;
    }

    public AtomicBoolean isWorking() {
        return isWorking;
    }

    public List<Iterator<Entry<MemorySegment>>> getInMemoryIterators(MemorySegment from, MemorySegment to) {
        // TODO check that storage.stream.map() - is okey by performance
        if (from == null && to == null) {
            return storages.stream().map(map -> map.values().iterator()).toList();
        }
        if (from == null) {
            return storages.stream().map(map -> map.headMap(to).values().iterator()).toList();
        }
        if (to == null) {
            return storages.stream().map(map -> map.tailMap(from).values().iterator()).toList();
        }
        return storages.stream().map(map -> map.subMap(from, to).values().iterator()).toList();
    }

    public void flush() throws IOException {
        if (storages.get(0).isEmpty() || !isWorking.compareAndSet(false, true)) {
            return;
        }
        currentByteSize.set(0);

        final Path indexTmp = flushPath.resolve(NAME_TMP_INDEX_FILE);
        final Path indexFile = flushPath.resolve(NAME_INDEX_FILE);

        final ActualFilesInterval interval = DiskStorage.getActualFilesInterval(indexFile, arena);
        long newFileName = lastFileNumber.getAndIncrement();

        long dataSize = 0;
        long count = 0;
        for (Entry<MemorySegment> entry : storages.get(0).values()) {
            dataSize += entry.key().byteSize();
            MemorySegment value = entry.value();
            if (value != null) {
                dataSize += value.byteSize();
            }
            count++;
        }
        long indexSize = count * 2 * Long.BYTES;

        try (
                FileChannel fileChannel = FileChannel.open(flushPath.resolve(Long.toString(newFileName)),
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.CREATE
                )
        ) {
            MemorySegment fileSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, indexSize + dataSize, arena
            );

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : storages.get(0).values()) {
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
            for (Entry<MemorySegment> entry : storages.get(0).values()) {
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

        Files.move(indexFile, indexTmp, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        diskStorage.addNewList(newFileName);
        changeActualFilesInterval(indexFile, arena, interval.left(), newFileName + 1);

        Files.delete(indexTmp);
        Collections.swap(storages, 0, 1);
        storages.get(1).clear();
        isWorking.set(false);
    }

    public void upsert(Entry<MemorySegment> entry) {
        if (isWorking.get()) {
            upsertWhenFlushing(entry);
            return;
        }

        long size = entrySize(entry);
        storages.get(0).put(entry.key(), entry);
        if (currentByteSize.addAndGet(size) > flushThresholdBytes) {
            try {
                flush();
            } catch (IOException ignored) {
                System.err.println("Incorrect state");
            }
        }
    }

    private void upsertWhenFlushing(Entry<MemorySegment> entry) {
        long size = entrySize(entry);
        storages.get(1).put(entry.key(), entry);
        if (currentByteSize.addAndGet(size) > flushThresholdBytes) {
            throw new OutOfMemoryError("Can't upsert data while flushing");
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (storages.get(1).containsKey(key)) {
            return storages.get(1).get(key);
        }

        return storages.get(0).get(key);
    }

    private long entrySize(Entry<MemorySegment> entry) {
        return entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize()) + 2 * Long.BYTES;
    }
}
