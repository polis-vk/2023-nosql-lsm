package ru.vk.itmo.khadyrovalmasgali;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.test.khadyrovalmasgali.DaoFactoryImpl;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static ru.vk.itmo.khadyrovalmasgali.SSTable.*;

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
        return new MergeIterator(from, to);
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
            long dataSize = 0, indexesSize = (long) data.size() * Long.BYTES;
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

    private class MergeIterator implements Iterator<Entry<MemorySegment>> {

        private final ConcurrentSkipListMap<MemorySegment, MergeIteratorEntry> priorityMap;
        private final ArrayList<Iterator<Entry<MemorySegment>>> iters;
        private MergeIteratorEntry mEntry = null;

        public MergeIterator(MemorySegment from, MemorySegment to) {
            priorityMap = new ConcurrentSkipListMap<>(comparator);
            iters = new ArrayList<>();
            Iterator<Entry<MemorySegment>> inMemoryIt;
            if (from == null && to == null) {
                inMemoryIt = data.values().iterator();
            } else if (from == null) {
                inMemoryIt = data.headMap(to).values().iterator();
            } else if (to == null) {
                inMemoryIt = data.tailMap(from).values().iterator();
            } else {
                inMemoryIt = data.subMap(from, to).values().iterator();
            }
            inMemoryIt = new InMemoryIteratorWrapper(inMemoryIt);
            if (inMemoryIt.hasNext()) {
                Entry<MemorySegment> item = inMemoryIt.next();
                priorityMap.put(item.key(), new MergeIteratorEntry(item, 0));
            }
            iters.add(inMemoryIt);
            for (SSTable sstable : sstables) {
                Iterator<Entry<MemorySegment>> it = sstable.get(from, to);
                int index = iters.size();
                while (it.hasNext()) {
                    Entry<MemorySegment> item = it.next();
                    if (!priorityMap.containsKey(item.key())) {
                        priorityMap.put(item.key(), new MergeIteratorEntry(item, index));
                    }
                }
                iters.add(it);
            }
            updateEntry();
//            for (var e: priorityMap.values()) {
//                System.out.println(e.index + " === " + e.entry);
//            }
        }

        private void updateEntry() {
            mEntry = null;
            while (!priorityMap.isEmpty()) {
                MergeIteratorEntry item = priorityMap.pollFirstEntry().getValue();
                updateIter(iters.get(item.index), item.index);
                if (item.entry.value() != null) {
                    mEntry = item;
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return mEntry != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Merge Iterator has no elements left.");
            }
            Entry<MemorySegment> result = mEntry.entry;
            Iterator<Entry<MemorySegment>> iter = iters.get(mEntry.index);
            updateIter(iter, mEntry.index);
            updateEntry();
            return result;
        }

        private void updateIter(Iterator<Entry<MemorySegment>> iter, int index)
        {
            while (iter.hasNext()) {
                Entry<MemorySegment> next = iter.next();
                if (priorityMap.containsKey(next.key())) {
                    MergeIteratorEntry other = priorityMap.get(next.key());
                    if (other.index < index) {
                        continue;
                    } else {
                        updateIter(iters.get(other.index), other.index);
                    }
                }
                priorityMap.put(next.key(), new MergeIteratorEntry(next, index));
                break;
            }
        }

        private class MergeIteratorEntry {
            private final Entry<MemorySegment> entry;
            private final int index;
            public MergeIteratorEntry(Entry<MemorySegment> entry, int index) {
                this.entry = entry;
                this.index = index;
            }
//            public Entry<MemorySegment> entry() {
//                return entry;
//            }
        }
    }

    private static class InMemoryIteratorWrapper implements Iterator<Entry<MemorySegment>>
    {

        private final Iterator<Entry<MemorySegment>> it;
        private Entry<MemorySegment> entry = null;

        public InMemoryIteratorWrapper(Iterator<Entry<MemorySegment>> it)
        {
            this.it = it;
            getNextEntry();
        }

        @Override
        public boolean hasNext() {
            return entry != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = entry;
            getNextEntry();
            return result;
        }

        private void getNextEntry()
        {
            if (it.hasNext()) {
                entry = it.next();
            } else {
                entry = null;
            }
        }
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
        offset += Long.BYTES;
        long msize = msegment.byteSize();
        MemorySegment.copy(msegment, 0, mappedSegment, offset, msize);
        return offset + msize;
    }

    public static void main(String[] args) throws IOException {
        Path path = Path.of(System.getProperty("user.dir")).resolve("main_build");
        Files.createDirectories(path);
        Dao<String, Entry<String>> dao = new DaoFactoryImpl().createStringDao(new Config(path));
        dao.upsert(e("k1", "v1"));
        dao.upsert(e("k2", "v2"));
        dao.upsert(e("k3", "v3"));
        dao.upsert(e("k4", "v4"));
        dao.upsert(e("k5", "v5"));
        dao.upsert(e("k3", null));
        dao.upsert(e("k4", null));
        int count = 0;
        for (var it = dao.all(); it.hasNext();) {
            System.out.println(it.next());
            ++count;
        }
        System.out.println(count);
        dao.close();
    }

    public static Entry<String> e(String k, String v) {return new BaseEntry<>(k, v); }
}
