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
import java.nio.file.Path;
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

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final Logger logger = Logger.getLogger(PersistentDao.class.getName());
    private final Config config;
    private final Arena arena;
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> data;
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
    private final List<SSTable> sstables;

    public PersistentDao(final Config config) {
        this.config = config;
        arena = Arena.ofShared();
        data = new ConcurrentSkipListMap<>(comparator);
        sstables = loadData();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        return new MergeIterator(from, to, data, sstables);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        if (data.containsKey(key)) {
            Entry<MemorySegment> result = data.get(key);
            return result.value() == null ? null : result;
        } else {
            return findValueInSSTable(key);
        }
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
        long timestamp = System.currentTimeMillis();
        try (FileChannel dataChannel = getFileChannel(SSTABLE_NAME_PREFIX, timestamp);
             FileChannel indexesChannel = getFileChannel(INDEX_NAME_PREFIX, timestamp);
             FileChannel metaChanel = getFileChannel(META_NAME_PREFIX, timestamp)) {
            long dataSize = 0;
            long indexesSize = (long) data.size() * Long.BYTES;
            for (Entry<MemorySegment> entry : data.values()) {
                dataSize += entry.key().byteSize() + 2 * Long.BYTES;
                if (entry.value() != null) {
                    dataSize += entry.value().byteSize();
                }
            }
            MemorySegment mappedData = dataChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataSize, arena);
            MemorySegment mappedIndexes = indexesChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexesSize, arena);
            MemorySegment mappedMeta = metaChanel.map(FileChannel.MapMode.READ_WRITE, 0, Long.BYTES, arena);
            mappedMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, data.size());
            long dataOffset = 0;
            long indexesOffset = 0;
            for (var entry : data.values()) {
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

    @Override
    public void close() throws IOException {
        flush();
        if (arena.scope().isAlive()) {
            arena.close();
        }
        data.clear();
    }

    private FileChannel getFileChannel(String indexNamePrefix, long timestamp) throws IOException {
        return FileChannel.open(
                config.basePath().resolve(String.format("%s%d", indexNamePrefix, timestamp)),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ);
    }

    private List<SSTable> loadData() {
        File[] files = config.basePath().toFile().listFiles((dir, name) -> name.startsWith(SSTABLE_NAME_PREFIX));
        ArrayList<SSTable> result = new ArrayList<>();
        if (files != null) {
            for (File f : files) {
                String timestamp = f.getName().substring(2);
                result.add(new SSTable(config.basePath(), timestamp, logger, arena));
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

//    public static void main(String[] args) {
//        Path path = Path.of(System.getProperty("user.dir")).resolve("main_build");
//        Config config = new Config(path);
//        Dao<String, Entry<String>>
//    }
}
