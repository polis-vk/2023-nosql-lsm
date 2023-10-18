package ru.vk.itmo.boturkhonovkamron.persistence;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.boturkhonovkamron.util.Offset;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static ru.vk.itmo.boturkhonovkamron.MemorySegmentComparator.COMPARATOR;
import static ru.vk.itmo.boturkhonovkamron.persistence.SSTable.INDEX_FILE;
import static ru.vk.itmo.boturkhonovkamron.persistence.SSTable.TABLE_FILE;

public class PersistentDao {

    private static final Set<StandardOpenOption> READ_WRITE_OPTIONS = Set.of(READ, WRITE, CREATE);

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> inMemory;

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> inMemoryDeleted;

    /**
     * Stores all SSTables in descending order of their versions.
     */
    private final List<SSTable> ssTables = new ArrayList<>();

    private final Path basePath;

    public PersistentDao(final Config config,
            final NavigableMap<MemorySegment, Entry<MemorySegment>> data,
            final NavigableMap<MemorySegment, Entry<MemorySegment>> deletedData) throws IOException {
        this.basePath = config.basePath();
        this.inMemory = data;
        this.inMemoryDeleted = deletedData;
        final NavigableSet<Long> versions;
        try (final Stream<Path> list = Files.list(basePath)) {
            versions = list.filter(Files::isDirectory)
                    .map(path -> Long.parseLong(String.valueOf(path.getFileName())))
                    .collect(Collectors.toCollection(TreeSet::new));
        }
        for (final Long version : versions.descendingSet()) {
            ssTables.add(new SSTable(basePath, version));
        }
    }

    public Entry<MemorySegment> getEntity(final MemorySegment key) {
        if (inMemoryDeleted.containsKey(key)) {
            return null;
        }
        for (final SSTable ssTable : ssTables) {
            final Entry<MemorySegment> entity = ssTable.getEntity(key);
            if (entity != null && entity.value() != null) {
                return entity;
            }
        }
        return null;
    }

    public void saveData(final NavigableMap<MemorySegment, Entry<MemorySegment>> data) throws IOException {
        for (final SSTable ssTable : ssTables) {
            if (!ssTable.close()) {
                return;
            }
        }
        final String version = String.valueOf(System.currentTimeMillis());
        final Path versionDir = Files.createDirectory(basePath.resolve(version));
        final Path indexPath = versionDir.resolve(INDEX_FILE);
        final Path tablePath = versionDir.resolve(TABLE_FILE);

        try (Arena indexArena = Arena.ofConfined();
                Arena tableArena = Arena.ofConfined();
                FileChannel indexChannel = FileChannel.open(indexPath, READ_WRITE_OPTIONS);
                FileChannel tableChannel = FileChannel.open(tablePath, READ_WRITE_OPTIONS)) {

            final long indexFileSize = (long) data.size() * Long.BYTES;
            final long tableFileSize = data.values().stream().mapToLong(entry -> {
                final long valueSize = entry.value() == null ? 0L : entry.value().byteSize();
                return Long.BYTES * 2 + valueSize + entry.key().byteSize();
            }).sum();

            final MemorySegment indexMap =
                    indexChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexFileSize, indexArena);
            final MemorySegment tableMap =
                    tableChannel.map(FileChannel.MapMode.READ_WRITE, 0, tableFileSize, tableArena);

            final Offset indexOffset = new Offset(0);
            final Offset tableOffset = new Offset(0);
            data.forEach((key, entry) -> {
                final long keySize = entry.key().byteSize();
                final long valueSize = entry.value() == null ? 0L : entry.value().byteSize();

                // Saving current entry offset to index file
                indexMap.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset.getAndAdd(Long.BYTES), tableOffset.get());

                // Saving key size and key to table file
                tableMap.set(ValueLayout.JAVA_LONG_UNALIGNED, tableOffset.getAndAdd(Long.BYTES), keySize);
                MemorySegment.copy(entry.key(), 0, tableMap, tableOffset.getAndAdd(keySize), keySize);

                // Saving value size and value to table file
                tableMap.set(ValueLayout.JAVA_LONG_UNALIGNED, tableOffset.getAndAdd(Long.BYTES), valueSize);
                if (valueSize > 0) {
                    MemorySegment.copy(entry.value(), 0, tableMap, tableOffset.getAndAdd(valueSize), valueSize);
                }
            });

        }
    }

    public Iterator<Entry<MemorySegment>> getIterator(final MemorySegment from, final MemorySegment to) {
        return new SSTableIterator(from, to);
    }

    private class SSTableIterator implements Iterator<Entry<MemorySegment>> {

        private final List<Long> positions;

        private final MemorySegment to;

        private MemorySegment lastKey;

        private Entry<MemorySegment> next;

        public SSTableIterator(final MemorySegment from, final MemorySegment to) {
            this.to = to;
            this.positions = new ArrayList<>(Collections.nCopies(ssTables.size(), 0L));
            if (from != null) {
                // Setting initial positions
                for (final SSTable ssTable : ssTables) {
                    final long keyPosition = ssTable.searchKeyPosition(from);
                    positions.add(keyPosition < 0 ? -keyPosition - 1 : keyPosition);
                }
            }
            advance(from);
        }

        private void advance(final MemorySegment key) {
            final MemorySegment inMemKey; // Key with most priority
            if (inMemory.isEmpty()) {
                inMemKey = null;
            } else {
                if (key == null) {
                    // When "from" is null
                    inMemKey = inMemory.firstKey();
                } else {
                    if (lastKey == null) {
                        // If the last key is null, then the next key is at least "from"
                        inMemKey = inMemory.ceilingKey(key);
                    } else {
                        // If the last key is not null, then the next key must be greater than the last one
                        inMemKey = inMemory.higherKey(key);
                    }
                }
            }
            final BiPredicate<MemorySegment, MemorySegment> predicate; // Used to compare the given key with stored keys
            if (lastKey == null) {
                predicate = (left, right) -> COMPARATOR.compare(left, right) >= 0;
            } else {
                predicate = (left, right) -> COMPARATOR.compare(left, right) > 0;
            }
            MemorySegment nextKey = inMemKey;
            final Set<Integer> posToUpdate = new HashSet<>(); // Positions in files to be updated
            for (int i = 0; i < ssTables.size(); i++) {
                final long index = positions.get(i);
                final SSTable table = ssTables.get(i);
                if (index >= table.size()) {
                    continue;
                }
                final MemorySegment value = table.valueAt(index);
                final MemorySegment currKey = table.keyAt(index);
                if (value == null || inMemoryDeleted.containsKey(currKey)) {
                    // If entry is deleted
                    posToUpdate.add(i);
                    continue;
                }
                if (predicate.test(currKey, key)) {
                    if (nextKey == null) {
                        nextKey = currKey;
                    } else {
                        final int cmp = COMPARATOR.compare(currKey, nextKey);
                        if (cmp < 0) {
                            nextKey = currKey;
                            posToUpdate.clear();
                            posToUpdate.add(i);
                        } else if (cmp == 0) {
                            posToUpdate.add(i);
                        }
                    }
                }
            }
            posToUpdate.forEach(index -> positions.set(index, positions.get(index) + 1));
            if (nextKey == null) {
                next = null;
            } else if (to != null) {
                final int cmp = COMPARATOR.compare(nextKey, to);
                if (cmp < 0) {
                    next = getEntity(nextKey);
                } else if (cmp == 0 && lastKey == null) {
                    next = getEntity(nextKey);
                } else {
                    next = null;
                }
            } else {
                next = getEntity(nextKey);
            }
            lastKey = nextKey;
        }

        private Entry<MemorySegment> getEntity(final MemorySegment key) {
            if (inMemoryDeleted.containsKey(key)) {
                return null;
            }
            if (inMemory.containsKey(key)) {
                return inMemory.get(key);
            }
            return PersistentDao.this.getEntity(key);
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> res;
            if ((res = next) == null) {
                throw new NoSuchElementException();
            }
            advance(next.key());
            return res;
        }
    }
}
