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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static ru.vk.itmo.grunskiialexey.DiskStorage.changeActualFilesInterval;
import static ru.vk.itmo.grunskiialexey.DiskStorage.tombstone;

public class InMemoryQuerySystem {
    private static final String NAME_TMP_INDEX_FILE = "index.tmp";
    private static final String NAME_INDEX_FILE = "index.idx";
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage;
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> storage2;
    private final long flushThresholdBytes;
    private final AtomicLong lastFileNumber;
    private final Path flushPath;

    public InMemoryQuerySystem(Path flushPath, long flushThresholdBytes, Comparator<MemorySegment> comparator, AtomicLong lastFileNumber) {
        this.storage = new ConcurrentSkipListMap<>(comparator);
        this.storage2 = new ConcurrentSkipListMap<>(comparator);

        this.flushPath = flushPath;
        this.flushThresholdBytes = flushThresholdBytes;
        this.lastFileNumber = lastFileNumber;
    }

    public List<Iterator<Entry<MemorySegment>>> getInMemoryIterators(MemorySegment from, MemorySegment to) {
        if (from == null && to == null) {
            return List.of(storage.values().iterator(), storage2.values().iterator());
        }
        if (from == null) {
            return List.of(
                    storage.headMap(to).values().iterator(),
                    storage2.headMap(to).values().iterator()
            );
        }
        if (to == null) {
            return List.of(
                    storage.tailMap(from).values().iterator(),
                    storage2.tailMap(from).values().iterator()
            );
        }
        return List.of(
                storage.subMap(from, to).values().iterator(),
                storage2.subMap(from, to).values().iterator()
        );
    }

    public void flush()
            throws IOException {
        if (storage.isEmpty()) {
            return;
        }


        final Path indexTmp = flushPath.resolve(NAME_TMP_INDEX_FILE);
        final Path indexFile = flushPath.resolve(NAME_INDEX_FILE);

        final ActualFilesInterval interval;
        try (final Arena arena = Arena.ofShared()) {
            interval = DiskStorage.getActualFilesInterval(indexFile, arena);
        }

        long newFileName = lastFileNumber.getAndIncrement();

        long dataSize = 0;
        long count = 0;
        for (Entry<MemorySegment> entry : storage.values()) {
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
                );
                Arena writeArena = Arena.ofConfined()
        ) {
            MemorySegment fileSegment = fileChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, indexSize + dataSize, writeArena
            );

            // index:
            // |key0_Start|value0_Start|key1_Start|value1_Start|key2_Start|value2_Start|...
            // key0_Start = data start = end of index
            long dataOffset = indexSize;
            int indexOffset = 0;
            for (Entry<MemorySegment> entry : storage.values()) {
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
            for (Entry<MemorySegment> entry : storage.values()) {
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

        try (final Arena writeArena = Arena.ofShared()) {
            changeActualFilesInterval(indexFile, writeArena, interval.left(), newFileName + 1);
        }

        Files.delete(indexTmp);
    }

    public void upsert(Entry<MemorySegment> entry) {
        //        if (flush.isReachedThreshold(entry) && flush.isWorking().get()) {
//            try {
//                Thread.sleep(10);
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
//        if (flush.isReachedThreshold(entry) && flush.isWorking().get()) {
//            throw new OutOfMemoryError("Can't upsert data in flushing file");
//        }
//
//        if (flush.isReachedThreshold(entry)) {
//            try {
//                flush();
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
//
//        if (flush.isWorking().get() && isUpserting.compareAndSet(false, true)) {
//            flushStorage.put(entry.key(), entry);
//            isUpserting.set(false);
//        }
        storage.put(entry.key(), entry);
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        return storage.get(key);
    }
}
