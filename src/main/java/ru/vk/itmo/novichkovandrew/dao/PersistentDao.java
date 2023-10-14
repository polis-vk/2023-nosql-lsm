package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.table.SortedStringTableMap;
import ru.vk.itmo.novichkovandrew.table.TableMap;
import ru.vk.itmo.novichkovandrew.Utils;
import ru.vk.itmo.novichkovandrew.iterator.PeekTableIterator;

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

import static ru.vk.itmo.novichkovandrew.Utils.copyToSegment;


public class PersistentDao extends InMemoryDao {
    /**
     * Path associated with SSTables.
     */
    private final Path path;

    private final String SST_NAME = "data";
    private final String SST_FORMAT = "txt";

    private final Arena arena;
    private final StandardOpenOption[] openOptions = new StandardOpenOption[]{
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
    };

    private final List<TableMap<MemorySegment, MemorySegment>> maps;

    public PersistentDao(Path path) {
        this.path = path;
        this.arena = Arena.ofConfined();
        this.maps = new ArrayList<>();
        maps.add(memTable);
    }


    @Override
    public void flush() throws IOException {
        try {
            Path sstPath = sstTablePath(Utils.filesCount(path) + 1);
            try (FileChannel sst = FileChannel.open(sstPath, openOptions)) {
                long metaSize = memTable.getMetaDataSize();
                long sstOffset = 0L;
                long indexOffset = Utils.writeLong(sst, 0L, memTable.size());
                MemorySegment sstMap = sst.map(FileChannel.MapMode.READ_WRITE, metaSize, memTable.byteSize(), arena);
                for (Entry<MemorySegment> entry : memTable) {
                    long keyOffset = sstOffset + metaSize;
                    long valueOffset = keyOffset + entry.key().byteSize();
                    indexOffset = writePosToFile(sst, indexOffset, keyOffset, valueOffset);
                    sstOffset = copyToSegment(sstMap, entry.key(), sstOffset);
                    sstOffset = copyToSegment(sstMap, entry.value(), sstOffset);
                }
                writePosToFile(sst, indexOffset, sstOffset + metaSize, 0L);
            }
        } catch (InvalidPathException ex) {
            throw new RuntimeException(
                    String.format("Failed by path with pattern %s-n.%s, %s%n", SST_NAME, SST_FORMAT, ex.getMessage())
            );
        }
    }

    @Override
    public void close() throws IOException {
        if (!memTable.isEmpty()) {
            flush();
        }
        for (var fileMap : maps) {
            fileMap.close();
        }
        maps.clear();
        if (arena.scope().isAlive()) {
            arena.close();
        }
        super.close();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        openAll();
        return new Iterator<>() {
            final Comparator<PeekTableIterator<MemorySegment>> iterComparator = (o1, o2) -> {
                int memoryComparison = comparator.compare(o1.peek(), o2.peek());
                if (memoryComparison == 0) {
                    return (-1) * Integer.compare(o1.getTableNumber(), o2.getTableNumber());
                }
                return memoryComparison;
            };
            private final TreeSet<PeekTableIterator<MemorySegment>> set = maps.stream()
                    .map(map -> map.iterator(from, to))
                    .filter(Iterator::hasNext)
                    .collect(Collectors.toCollection(() -> new TreeSet<>(iterComparator)));
            private MemorySegment minKey;
            private Entry<MemorySegment> minEntry = getNextEntry();

            private Entry<MemorySegment> getNextEntry() {
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
                        return mapNumber < 0 ? memTable.getEntry(key) : maps.get(mapNumber).getEntry(key);
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
            public Entry<MemorySegment> next() {
                var entry = minEntry;
                minEntry = getNextEntry();
                return entry;
            }


        };
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> entry;
        if ((entry = super.get(key)) != null) {
            return entry;
        }
        int filesCount = Utils.filesCount(path);
        for (int i = filesCount; i >= 1; i--) {
            TableMap<MemorySegment, MemorySegment> map;
            int position = filesCount - i + 1;
            if (position < maps.size()) {
                map = maps.get(position);
            } else {
                map = new SortedStringTableMap(sstTablePath(i), i, comparator, arena);
                maps.add(map);
            }
            MemorySegment sstKey = map.ceilKey(key);
            if (comparator.compare(sstKey, key) == 0) {
                return map.getEntry(sstKey);
            }
        }
        return null;
    }


    private void openAll() {
        int filesCount = Utils.filesCount(path);
        final int alreadyOpened = maps.size() - 1;
        for (int i = filesCount; i >= 1; i--) {
            if (filesCount - i >= alreadyOpened) {
                Path sstPath = sstTablePath(i);
                maps.add(new SortedStringTableMap(sstPath, i, comparator, arena));
            }
        }
    }

    private long writePosToFile(FileChannel channel, long offset, long keyOff, long valOff) {
        offset = Utils.writeLong(channel, offset, keyOff);
        offset = Utils.writeLong(channel, offset, valOff);
        return offset;
    }

    private Path sstTablePath(long suffix) {
        String fileName = String.format("%s-%s.%s", SST_NAME, suffix, SST_FORMAT);
        return path.resolve(Path.of(fileName));
    }
}
