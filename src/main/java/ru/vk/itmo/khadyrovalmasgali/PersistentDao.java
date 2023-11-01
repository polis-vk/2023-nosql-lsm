package ru.vk.itmo.khadyrovalmasgali;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

import static ru.vk.itmo.khadyrovalmasgali.SSTable.INDEX_NAME_PREFIX;
import static ru.vk.itmo.khadyrovalmasgali.SSTable.META_NAME_PREFIX;
import static ru.vk.itmo.khadyrovalmasgali.SSTable.SSTABLE_NAME_PREFIX;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>>, Iterable<Entry<MemorySegment>> {

    private long tableCount;
    private final Config config;
    private final Arena arena;
    private final List<SSTable> sstables;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data;
    private static final Logger logger = Logger.getLogger(PersistentDao.class.getName());
    public static final Comparator<MemorySegment> comparator = (o1, o2) -> {
        long mismatch = o1.mismatch(o2);
        if (mismatch == o2.byteSize()) {
            return 1;
        } else if (mismatch == o1.byteSize()) {
            return -1;
        } else if (mismatch == -1) {
            return 0;
        }
        return Byte.compare(
                o1.get(ValueLayout.JAVA_BYTE, mismatch),
                o2.get(ValueLayout.JAVA_BYTE, mismatch));
    };

    public PersistentDao(final Config config) {
        this.config = config;
        tableCount = 1; // reserving 0 & 1 for compact operation
        arena = Arena.ofShared();
        data = new ConcurrentSkipListMap<>(comparator);
        sstables = loadData();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        return new MergeIterator(from, to, data, sstables, comparator);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Entry<MemorySegment> result = data.get(key);
        if (result == null) {
            return findValueInSSTable(key);
        }
        return result.value() == null ? null : result;
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        data.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        if (data.isEmpty()) {
            return;
        }
        flush(data.values(), tableCount + 1);
    }

    @Override
    public void close() throws IOException {
        flush();
        if (arena.scope().isAlive()) {
            arena.close();
        }
        data.clear();
    }

    private void flush(Iterable<Entry<MemorySegment>> iterable, long tableNum) throws IOException {
        try (FileChannel dataChannel = getFileChannel(SSTABLE_NAME_PREFIX, tableNum);
             FileChannel indexesChannel = getFileChannel(INDEX_NAME_PREFIX, tableNum);
             FileChannel metaChanel = getFileChannel(META_NAME_PREFIX, tableNum)) {
            long indexesSize = 0;
            long dataSize = 0;
            for (Entry<MemorySegment> entry : iterable) {
                dataSize += entry.key().byteSize() + 2 * Long.BYTES;
                ++indexesSize;
                if (entry.value() != null) {
                    dataSize += entry.value().byteSize();
                }
            }
            MemorySegment mappedData = dataChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, dataSize, arena);
            MemorySegment mappedIndexes = indexesChannel.map(
                    FileChannel.MapMode.READ_WRITE, 0, indexesSize * Long.BYTES, arena);
            MemorySegment mappedMeta = metaChanel.map(
                    FileChannel.MapMode.READ_WRITE, 0, Long.BYTES, arena);
            mappedMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, indexesSize);
            long dataOffset = 0;
            long indexesOffset = 0;
            for (var entry : iterable) {
                mappedIndexes.set(ValueLayout.JAVA_LONG_UNALIGNED, indexesOffset, dataOffset);
                indexesOffset += Long.BYTES;
                dataOffset = writeSegment(mappedData, dataOffset, entry.key());
                dataOffset = writeSegment(mappedData, dataOffset, entry.value());
            }
            mappedData.load();
            mappedIndexes.load();
            mappedMeta.load();
        }
    }

    private FileChannel getFileChannel(String indexNamePrefix, long tableNum) throws IOException {
        return FileChannel.open(
                config.basePath().resolve(String.format("%s%d", indexNamePrefix, tableNum)),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);
    }

    private List<SSTable> loadData() {
        File[] files = config.basePath().toFile().listFiles((dir, name) -> name.startsWith(SSTABLE_NAME_PREFIX));
        ArrayList<SSTable> result = new ArrayList<>();
        if (files != null) {
            for (File f : files) {
                String tableNum = f.getName().substring(2);
                try {
                    tableCount = Math.max(Long.parseLong(tableNum), tableCount);
                    result.add(new SSTable(config.basePath(), tableNum, logger, arena));
                } catch (NumberFormatException ignored) {
                    // non-db files
                }
            }
        }
        result.sort(SSTable::compareTo);
        return result;
    }

    private Entry<MemorySegment> findValueInSSTable(final MemorySegment key) {
        for (SSTable sstable : sstables) {
            Entry<MemorySegment> result = sstable.get(key);
            if (result != null) {
                if (result.value() != null) {
                    return result;
                }
                return null;
            }
        }
        return null;
    }

    private long writeSegment(final MemorySegment mappedSegment, long offset, final MemorySegment msegment) {
        if (msegment == null) {
            mappedSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1L);
            return offset + Long.BYTES;
        }
        mappedSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, msegment.byteSize());
        long msize = msegment.byteSize();
        MemorySegment.copy(msegment, 0, mappedSegment, offset + Long.BYTES, msize);
        return offset + Long.BYTES + msize;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return all();
    }
}
