package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.Utils;
import ru.vk.itmo.novichkovandrew.iterator.IteratorsComparator;
import ru.vk.itmo.novichkovandrew.iterator.PeekTableIterator;
import ru.vk.itmo.novichkovandrew.table.SortedStringTableMap;
import ru.vk.itmo.novichkovandrew.table.TableMap;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ru.vk.itmo.novichkovandrew.Utils.copyToSegment;


public class PersistentDao extends InMemoryDao {
    /**
     * Path associated with SSTables.
     */
    private final Path path;
    //    private final Arena arena;
    private final StandardOpenOption[] openOptions = new StandardOpenOption[]{
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
    };

    private final List<TableMap<MemorySegment, MemorySegment>> maps;

    public PersistentDao(Path path) {
        this.path = path;
        this.maps = new ArrayList<>();
        maps.add(memTable);
    }


    @Override
    public void flush() throws IOException {
        try {
            Path sstPath = sstTablePath(Utils.filesCount(path) + 1);
            try (FileChannel sst = FileChannel.open(sstPath, openOptions);
                 Arena arena = Arena.ofConfined()
            ) {
                long metaSize = memTable.getMetaDataSize();
                long sstOffset = 0L;
                long indexOffset = Utils.writeLong(sst, 0L, memTable.size());
                MemorySegment sstMap = sst.map(FileChannel.MapMode.READ_WRITE, metaSize, memTable.byteSize(), arena);
                for (Entry<MemorySegment> entry : memTable) {
                    long keyOffset = sstOffset + metaSize;
                    long valueOffset = keyOffset + entry.key().byteSize();
                    if (memTable.isTombstone(entry)) {
                        valueOffset *= -1;
                    }
                    indexOffset = writePosToFile(sst, indexOffset, keyOffset, valueOffset);
                    sstOffset = copyToSegment(sstMap, entry.key(), sstOffset);
                    sstOffset = copyToSegment(sstMap, entry.value(), sstOffset);
                }
                writePosToFile(sst, indexOffset, sstOffset + metaSize, 0L);
            }
        } catch (InvalidPathException ex) {
            throw new RuntimeException("Failed by path: " + ex.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.size() != 0) {
            flush();
        }
        for (var fileMap : maps) {
            fileMap.close();
        }
        maps.clear();
        super.close();
    }

    @Override
    public Iterator<ru.vk.itmo.Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        openAll();
        if (maps.size() == 1) {
            return PersistentDao.super.get(from, to);
        }
        Stream<PeekTableIterator<MemorySegment>> stream = maps.stream()
                .map(m -> m.keyIterator(from, to))
                .filter(PeekTableIterator::hasNext);
        return new SortedStingTablesIterator(stream);
    }

    @Override
    public ru.vk.itmo.Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry = memTable.getEntry(key);
        if (entry != null) {
            return memTable.isTombstone(entry) ? null : entry;
        }
        int filesCount = Utils.filesCount(path);
        for (int i = filesCount; i >= 1; i--) {
            TableMap<MemorySegment, MemorySegment> map;
            int position = filesCount - i + 1;
            if (position < maps.size()) {
                map = maps.get(position);
            } else {
                map = new SortedStringTableMap(sstTablePath(i), i, comparator);
                maps.add(map);
            }
            MemorySegment sstKey = map.ceilKey(key);
            if (comparator.compare(sstKey, key) == 0) {
                entry = map.getEntry(sstKey);
                if (map.isTombstone(entry)) {
                    break;
                }
                return entry;
            }
        }
        return null;
    }


    public class SortedStingTablesIterator implements Iterator<ru.vk.itmo.Entry<MemorySegment>> {
        private final Comparator<PeekTableIterator<MemorySegment>> iterComparator;

        private final TreeSet<PeekTableIterator<MemorySegment>> set;
        private MemorySegment minKey;
        private ru.vk.itmo.Entry<MemorySegment> minEntry;

        public SortedStingTablesIterator(Stream<PeekTableIterator<MemorySegment>> sstIteratorsStream) {
            this.iterComparator = new IteratorsComparator<>(comparator);
            this.set = sstIteratorsStream.collect(Collectors.toCollection(() -> new TreeSet<>(iterComparator)));
            this.minEntry = getNextEntry();
        }

        private ru.vk.itmo.Entry<MemorySegment> getNextEntry() {
            while (!set.isEmpty()) {
                var iterator = set.pollFirst();
                if (iterator == null) {
                    continue;
                }
                var key = iterator.next();
                if (minKey == null || comparator.compare(key, minKey) > 0) {
                    minKey = key;
                    if (iterator.hasNext()) {
                        set.add(iterator);
                    }
                    int mapNumber = maps.size() - iterator.getTableNumber();
                    var entry = mapNumber < 0 ? memTable.getEntry(key) : maps.get(mapNumber).getEntry(key);
                    if (entry.value() != null) { // Fix it
                        return entry;
                    }
                } else {
                    if (iterator.hasNext()) {
                        set.add(iterator);
                    }
                }
            }
            return null;
        }

        @Override
        public boolean hasNext() {
            return minEntry != null;
        }

        @Override
        public ru.vk.itmo.Entry<MemorySegment> next() {
            var entry = minEntry;
            minEntry = getNextEntry();
            return entry;
        }
    }


    private void openAll() {
        int filesCount = Utils.filesCount(path);
        final int alreadyOpened = maps.size() - 1;
        for (int i = filesCount; i >= 1; i--) {
            if (filesCount - i >= alreadyOpened) {
                Path sstPath = sstTablePath(i);
                maps.add(new SortedStringTableMap(sstPath, i, comparator));
            }
        }
    }

    private long writePosToFile(FileChannel channel, long offset, long keyOff, long valOff) {
        offset = Utils.writeLong(channel, offset, keyOff);
        offset = Utils.writeLong(channel, offset, valOff);
        return offset;
    }

    private Path sstTablePath(long suffix) {
        String fileName = String.format("data-%s.txt", suffix);
        return path.resolve(Path.of(fileName));
    }
}
