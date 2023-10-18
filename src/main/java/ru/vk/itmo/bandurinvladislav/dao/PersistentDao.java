package ru.vk.itmo.bandurinvladislav.dao;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.bandurinvladislav.comparator.MemorySegmentComparator;
import ru.vk.itmo.bandurinvladislav.iterator.FileSegmentIterator;
import ru.vk.itmo.bandurinvladislav.iterator.MemTableIterator;
import ru.vk.itmo.bandurinvladislav.iterator.MemorySegmentIterator;
import ru.vk.itmo.bandurinvladislav.iterator.MergeIterator;
import ru.vk.itmo.bandurinvladislav.util.Constants;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final MemorySegmentComparator MEMORY_SEGMENT_COMPARATOR = new MemorySegmentComparator();

    private static final String META_INFO_STORAGE_NAME = "metaInfoStorage";
    private static final String SSTABLE_NAME_PATTERN = "SSTable%d";
    private static final String INDEX_NAME_SUFFIX_PATTERN = "-index%d";

    private final Arena daoArena = Arena.ofConfined();
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryStorage =
            new ConcurrentSkipListMap<>(MEMORY_SEGMENT_COMPARATOR);
    private final ArrayList<MemorySegment> sstables = new ArrayList<>();
    private final ArrayList<MemorySegment> indexes = new ArrayList<>();

    private final Path metaInfoStorage;
    private final Path sstable;
    private final Path index;
    private final MemorySegment metaInfoSegment;

    public PersistentDao(Config config) {
        metaInfoStorage = config.basePath().resolve(META_INFO_STORAGE_NAME);
        if (!Files.exists(metaInfoStorage)) {
            try {
                Files.createFile(metaInfoStorage);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        try (FileChannel fileChannel = FileChannel.open(metaInfoStorage,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE)) {
            metaInfoSegment = fileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    fileChannel.size(),
                    daoArena
            );

            if (fileChannel.size() > 0) {
                int sstableCount = metaInfoSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
                for (int i = 0; i < sstableCount; i++) {

                    Path tablePath = config.basePath().resolve(SSTABLE_NAME_PATTERN.formatted(i));
                    Path indexPath = config.basePath().resolve(INDEX_NAME_SUFFIX_PATTERN.formatted(i));

                    try (FileChannel sstableChannel = FileChannel.open(tablePath, StandardOpenOption.READ);
                         FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
                        sstables.add(sstableChannel.map(
                                FileChannel.MapMode.READ_ONLY,
                                0,
                                sstableChannel.size(),
                                daoArena));

                        indexes.add(indexChannel.map(
                                FileChannel.MapMode.READ_ONLY,
                                0,
                                indexChannel.size(),
                                daoArena));
                    }
                }
                sstable = config.basePath().resolve(SSTABLE_NAME_PATTERN.formatted(sstableCount));
                index = config.basePath().resolve(INDEX_NAME_SUFFIX_PATTERN.formatted(sstableCount));
            } else {
                sstable = config.basePath().resolve(SSTABLE_NAME_PATTERN.formatted(0));
                index = config.basePath().resolve(INDEX_NAME_SUFFIX_PATTERN.formatted(0));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return getEntryIterator(from, to);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> memorySegmentEntry = inMemoryStorage.get(key);

        if (memorySegmentEntry != null && memorySegmentEntry.value() == null) {
            return null;
        }
        if (memorySegmentEntry != null) {
            return memorySegmentEntry;
        }

        for (int i = sstables.size() - 1; i >= 0; i--) {
            MemorySegment sstableSegment = sstables.get(i);
            MemorySegment index = indexes.get(i);

            long entryIndex = findEntryIndex(sstableSegment, index, key);
            long keySize = index.get(ValueLayout.JAVA_LONG_UNALIGNED, entryIndex + Constants.INDEX_ROW_KEY_LENGTH_POSITION);
            long valueSize = index.get(ValueLayout.JAVA_LONG_UNALIGNED, entryIndex + Constants.INDEX_ROW_VALUE_LENGTH_POSITION);
            long offsetInFileSegment = index.get(ValueLayout.JAVA_LONG_UNALIGNED, entryIndex + Constants.INDEX_ROW_OFFSET_POSITION);
            MemorySegment entryKey = sstableSegment.asSlice(offsetInFileSegment, keySize);

            if (entryKey.mismatch(key) == -1) {
                MemorySegment entryValue = sstableSegment.asSlice(offsetInFileSegment + keySize, valueSize);
                return new BaseEntry<>(entryKey, entryValue);
            }
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        inMemoryStorage.put(entry.key(), entry);
    }

    private Iterator<Entry<MemorySegment>> getEntryIterator(MemorySegment fromKey, MemorySegment toKey) {
        ArrayList<MemorySegmentIterator> iteratorList = new ArrayList<>();
        Iterator<Entry<MemorySegment>> iteratorInRange = getIteratorInRange(fromKey, toKey);
        if (iteratorInRange.hasNext()) {
            iteratorList.add(new MemTableIterator(iteratorInRange));
        }

        for (int i = 0; i < sstables.size(); i++) {

            MemorySegment mappedSSTable = sstables.get(i);
            MemorySegment mappedIndex = indexes.get(i);

            long fromIndexOffset = fromKey == null ? 0 : findEntryIndex(mappedSSTable, mappedIndex, fromKey);
            long fromSSTableOffset = fromKey == null ? 0 : mappedIndex.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    fromIndexOffset + Constants.INDEX_ROW_OFFSET_POSITION);

            long toIndexOffset = toKey == null ? mappedIndex.byteSize() : findEntryIndex(mappedSSTable, mappedIndex, toKey);
            long toSSTableOffset = toKey == null ? mappedSSTable.byteSize() : mappedIndex.get(ValueLayout.JAVA_LONG_UNALIGNED,
                    toIndexOffset + Constants.INDEX_ROW_OFFSET_POSITION);

            iteratorList.add(new FileSegmentIterator(
                    mappedSSTable,
                    mappedIndex,
                    fromSSTableOffset, fromIndexOffset, toSSTableOffset,
                    sstables.size() - i
                    ));
        }
        if (iteratorList.isEmpty()) {
            return Collections.emptyIterator();
        }
        return new MergeIterator(iteratorList);
    }

    private Iterator<Entry<MemorySegment>> getIteratorInRange(MemorySegment from,
                                                              MemorySegment to) {
        if (from == null && to == null) {
            return inMemoryStorage.values().iterator();
        } else if (from == null) {
            return inMemoryStorage.headMap(to, false).values().iterator();
        } else if (to == null) {
            return inMemoryStorage.values().iterator();
        } else {
            return inMemoryStorage.subMap(from, true, to, false).values().iterator();
        }
    }

    private void writeMemorySegment(MemorySegment fileSegment, MemorySegment data, long offset) {
        MemorySegment.copy(data, 0, fileSegment, offset, data.byteSize());
    }

    @Override
    public void flush() throws IOException {
        try (FileChannel metaInfoChannel = FileChannel.open(metaInfoStorage,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel sstableChannel = FileChannel.open(sstable,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.READ,
                     StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel indexChannel = FileChannel.open(index,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.READ,
                     StandardOpenOption.TRUNCATE_EXISTING);
             Arena arena = Arena.ofConfined()) {

            long sstableSize = 0;
            long indexSize = Constants.INDEX_ROW_SIZE * inMemoryStorage.size();
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> e : inMemoryStorage.entrySet()) {
                sstableSize += e.getKey().byteSize() + e.getValue().value().byteSize();
            }

            MemorySegment sstableSegment = sstableChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    sstableSize,
                    arena
            );

            MemorySegment indexSegment = indexChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    indexSize,
                    arena
            );

            int counter = 1;
            long storageOffset = 0;
            long indexOffset = 0;
            for (Map.Entry<MemorySegment, Entry<MemorySegment>> e : inMemoryStorage.entrySet()) {
                indexSegment.set(ValueLayout.JAVA_INT_UNALIGNED, indexOffset, counter++);
                indexOffset += Integer.BYTES;
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, e.getKey().byteSize());
                indexOffset += Long.BYTES;
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, e.getValue().value().byteSize());
                indexOffset += Long.BYTES;
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, storageOffset);
                indexOffset += Long.BYTES;

                writeMemorySegment(sstableSegment, e.getKey(), storageOffset);
                storageOffset += e.getKey().byteSize();
                writeMemorySegment(sstableSegment, e.getValue().value(), storageOffset);
                storageOffset += e.getValue().value().byteSize();
            }

            MemorySegment metaInfoSegment = metaInfoChannel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0,
                    Integer.BYTES,
                    arena
            );
            metaInfoSegment.set(ValueLayout.JAVA_INT_UNALIGNED, 0, sstables.size() + 1);
//            metaInfoSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, metaOffset, minKey.byteSize());
//            metaOffset += Long.BYTES;
//            writeMemorySegment(metaInfoSegment, minKey, metaOffset);
//            metaOffset += minKey.byteSize();
//            metaInfoSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, metaOffset, maxKey.byteSize());
//            metaOffset += Long.BYTES;
//            writeMemorySegment(metaInfoSegment, maxKey, metaOffset);
        }
    }

    @Override
    public void close() throws IOException {
        if (daoArena.scope().isAlive()) {
            daoArena.close();
        }

        flush();
    }

    private long findEntryIndex(MemorySegment sstableSegment, MemorySegment indexSegment, MemorySegment key) {
        long l = 0;
        long r = indexSegment.byteSize();

        while (r - l > Constants.INDEX_ROW_SIZE) {
            long m = (l + r) / 2;
            m -= m % Constants.INDEX_ROW_SIZE;

            long keySize = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m + Constants.INDEX_ROW_KEY_LENGTH_POSITION);
            long offsetInFileSegment = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m + Constants.INDEX_ROW_OFFSET_POSITION);
            MemorySegment keyEntry = sstableSegment.asSlice(offsetInFileSegment, keySize);

            if (MEMORY_SEGMENT_COMPARATOR.compare(keyEntry, key) <= 0) {
                l = m;
            } else {
                r = m;
            }
        }
        return l;
    }
}
