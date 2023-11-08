package ru.vk.itmo.trofimovmaxim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final String FILENAME = "sstable";

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;
    private final Config config;
    private final SsTable ssTable;

    private static final Comparator<MemorySegment> COMPARE_SEGMENT = (o1, o2) -> {
        if (o1 == null || o2 == null) {
            return o1 == null ? -1 : 1;
        }

        long mism = o1.mismatch(o2);
        if (mism == -1) {
            return (int) (o1.byteSize() - o2.byteSize());
        }
        if (mism == o1.byteSize() || mism == o2.byteSize()) {
            return mism == o1.byteSize() ? -1 : 1;
        }
        return Byte.compare(
                o1.get(ValueLayout.OfByte.JAVA_BYTE, mism),
                o2.get(ValueLayout.OfByte.JAVA_BYTE, mism)
        );
    };

    public InMemoryDao() {
        memTable = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);
        config = null;
        ssTable = null;
    }

    public InMemoryDao(Config config) {
        this.config = config;
        memTable = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);
        SsTable sstable = new SsTable(config.basePath(), FILENAME);
        if (sstable.data == null || sstable.offsetsTable == null) {
            ssTable = null;
        } else {
            ssTable = sstable;
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null || to == null) {
            if (from == null && to == null) {
                return memTable.values().iterator();
            } else if (from == null) {
                return memTable.headMap(to).values().iterator();
            } else {
                return memTable.tailMap(from).values().iterator();
            }
        } else {
            return memTable.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var resultMemTable = memTable.get(key);
        if (resultMemTable != null) {
            return resultMemTable;
        }
        if (ssTable != null) {
            return ssTable.get(key);
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (config == null || ssTable == null || !ssTable.arena.scope().isAlive()) {
            return;
        }
        ssTable.arena.close();

        long size = 0;
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : memTable.entrySet()) {
            size += entry.getKey().byteSize() + entry.getValue().key().byteSize() + entry.getValue().value().byteSize();
        }

        try (Arena writeArena = Arena.ofConfined()) {
            try (FileChannel fileChannelData = FileChannel.open(config.basePath().resolve(FILENAME + ".data"),
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE);
                 FileChannel fileChannelMeta = FileChannel.open(config.basePath().resolve(FILENAME + ".meta"),
                         StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING,
                         StandardOpenOption.CREATE)) {
                MemorySegment pageData = fileChannelData.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);
                MemorySegment pageMeta = fileChannelMeta.map(FileChannel.MapMode.READ_WRITE, 0,
                        4L * memTable.size() * Long.BYTES, writeArena);

                long offsetData = 0;
                long offsetMeta = 0;

                for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : memTable.entrySet()) {
                    MemorySegment key = entry.getKey();

                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, offsetData);
                    offsetMeta += Long.BYTES;

                    long keySize = key.byteSize();
                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, keySize);
                    offsetMeta += Long.BYTES;

                    Entry<MemorySegment> val = entry.getValue();
                    long val1Size = val.key().byteSize();
                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, val1Size);
                    offsetMeta += Long.BYTES;

                    long val2Size = val.value().byteSize();
                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, val2Size);
                    offsetMeta += Long.BYTES;

                    MemorySegment.copy(key, 0, pageData, offsetData, keySize);
                    offsetData += key.byteSize();
                    MemorySegment.copy(val.key(), 0, pageData, offsetData, val1Size);
                    offsetData += val.key().byteSize();
                    MemorySegment.copy(val.value(), 0, pageData, offsetData, val2Size);
                    offsetData += val.value().byteSize();
                }
            }
        }
    }

    private static class SsTable {
        private MemorySegment data;
        private MemorySegment offsetsTable;
        private long size;
        private final Arena arena;

        SsTable(Path basePath, String filename) {
            Path pathToSsTable = basePath.resolve(filename + ".data");
            Path pathToOffsetsTable = basePath.resolve(filename + ".meta");

            arena = Arena.ofShared();

            Logger logger = Logger.getLogger(InMemoryDao.class.getName());
            try (FileChannel channelData = FileChannel.open(pathToSsTable, StandardOpenOption.READ);
                 FileChannel channelMeta = FileChannel.open(pathToOffsetsTable, StandardOpenOption.READ)) {
                long sizeData = Files.size(pathToSsTable);
                long sizeMeta = Files.size(pathToOffsetsTable);

                this.size = (sizeData / Long.BYTES / 4L);

                data = channelData.map(FileChannel.MapMode.READ_ONLY, 0, sizeData, arena);
                offsetsTable = channelMeta.map(FileChannel.MapMode.READ_ONLY, 0, sizeMeta, arena);
            } catch (FileNotFoundException e) {
                logger.log(Level.WARNING, String.format("File not found: %s", e));
            } catch (IOException e) {
                logger.log(Level.WARNING, String.format("Some IOException: %s", e));
            } catch (Exception e) {
                logger.log(Level.WARNING, String.format("Some exception: %s", e));
            }

            if (data == null || offsetsTable == null) {
                arena.close();
            }
        }

        Offset getOffset(long index) {
            long offset = 4L * index * Long.BYTES;
            long keyOffset = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            long keySize = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES);
            long val1Size = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 2L * Long.BYTES);
            long val2Size = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 3L * Long.BYTES);

            return new Offset(keyOffset, keySize, val1Size, val2Size);
        }

        Entry<MemorySegment> get(MemorySegment key) {
            long l = -1;
            long r = size;
            while (l < r - 1) {
                long m = (l + r) / 2;
                var offset = getOffset(m);
                MemorySegment dataM = data.asSlice(offset.keyOffset, offset.keySize);
                int cmp = COMPARE_SEGMENT.compare(dataM, key);
                if (cmp == 0) {
                    return new BaseEntry<>(
                            data.asSlice(offset.keyOffset + offset.keySize, offset.val1Size),
                            data.asSlice(offset.keyOffset + offset.keySize + offset.val1Size, offset.val2Size)
                    );
                }
                if (cmp < 0) {
                    l = m;
                } else {
                    r = m;
                }
            }

            return null;
        }

        static class Offset implements Serializable {
            private final long keyOffset;
            private final long keySize;
            private final long val1Size;
            private final long val2Size;

            public Offset(long keyOffset, long keySize, long val1Size, long val2Size) {
                this.keyOffset = keyOffset;
                this.keySize = keySize;
                this.val1Size = val1Size;
                this.val2Size = val2Size;
            }
        }
    }
}
