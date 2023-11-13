package ru.vk.itmo.kobyzhevaleksandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Storage {

    private static final String TABLE_FILENAME = "ssTable";
    private static final String TABLE_EXTENSION = ".dat";
    private static final String COMPACTED_TABLE_FILENAME = TABLE_FILENAME + "Compact" + TABLE_EXTENSION;
    private static final long NULL_SIZE = -1;
    private static final long ENTRY_COUNT_OFFSET = 0;
    private static final Logger logger = Logger.getLogger(Storage.class.getPackage().getName());
    private static final Pattern tablesPattern = Pattern.compile(TABLE_FILENAME + "\\d*" + TABLE_EXTENSION + "$");

    private final Arena arena = Arena.ofShared();
    private final Config config;
    private final List<MemorySegment> mappedSsTables;

    private boolean isCompacted;

    /*
    Filling ssTable with bytes from the memory segment with a structure:
    [entry_count]{[entry_pos]...}{[key_size][key][value_size][value]}...

    If value is null then value_size = -1
    */
    public Storage(Config config) {
        this.config = config;
        mappedSsTables = new ArrayList<>();
        Path tablesDir = config.basePath();

        if (!Files.exists(tablesDir)) {
            logger.log(Level.WARNING, "Can''t find the file {0}", tablesDir);
            return;
        }
        try (Stream<Path> files = Files.list(tablesDir)) {
            files
                .filter(path -> path.toString().endsWith(TABLE_EXTENSION))
                .sorted(Collections.reverseOrder())
                .forEach(tablePath -> {
                    try {
                        long size = Files.size(tablePath);
                        mappedSsTables.add(mapFile(tablePath, size, FileChannel.MapMode.READ_ONLY, arena,
                            StandardOpenOption.READ));
                    } catch (IOException e) {
                        logger.log(Level.SEVERE, "Can''t find the file {0}", tablePath);
                        throw new ApplicationException("Can't access the file", e);
                    }
                });
        } catch (IOException e) {
            throw new ApplicationException("Can't access the file", e);
        }
        mappedSsTables.reversed();
    }

    public Iterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(mappedSsTables.size());
        for (MemorySegment mappedSsTable : mappedSsTables) {
            long fromPos;
            long toPos;
            if (from == null) {
                fromPos = 0;
            } else {
                fromPos = binarySearchIndex(mappedSsTable, from);
            }
            if (to == null) {
                toPos = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ENTRY_COUNT_OFFSET);
            } else {
                toPos = binarySearchIndex(mappedSsTable, to);
            }
            iterators.add(
                new Iterator<>() {
                    long pos = fromPos;

                    @Override
                    public boolean hasNext() {
                        return pos < toPos;
                    }

                    @Override
                    public Entry<MemorySegment> next() {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }

                        return getEntryByIndex(mappedSsTable, pos++);
                    }
                }
            );
        }

        return GlobalIterator.merge(iterators);
    }

    public void save(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();

        if (entries.isEmpty() || isCompacted) {
            return;
        }

        Path tablePath = getTablePathForIndex(mappedSsTables.size());
        saveOnDisk(entries, tablePath);
    }

    public void compact(Iterable<Entry<MemorySegment>> iterable) throws IOException {
        if (!arena.scope().isAlive() || isCompacted || !iterable.iterator().hasNext()) {
            return;
        }

        Path compactedTablePath = config.basePath().resolve(COMPACTED_TABLE_FILENAME);
        saveOnDisk(iterable, compactedTablePath);
        isCompacted = true;

        try (Stream<Path> files = Files.list(config.basePath())) {
            files
                .filter(path -> tablesPattern.matcher(path.toString()).find())
                .forEach(tablePath -> {
                    try {
                        Files.delete(tablePath);
                    } catch (IOException e) {
                        throw new ApplicationException("Can't delete file", e);
                    }
                });
        }

        Path tablePath = getTablePathForIndex(0);
        Files.move(compactedTablePath, tablePath, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Saving to disk is done using {@link Iterable}. Iterable allows you to return multiple iterators.
     * In our case, the first iterator is used to calculate the size of the resulting ssTable and the number of entries,
     * and the second one is used to write segments directly to the mapped ssTable.
     *
     * <p>In addition, using Iterable allows you to make the {@code saveOnDisk()} method universal,
     * since saving is performed in two cases: when calling {@link PersistentDao#close()},
     * that is, all entries from {@link java.util.NavigableMap} are written,
     * and also when {@link PersistentDao#compact()}, which involves the use of iterators.
     *
     * @param iterable data to be saved on disk
     * @param tablePath path to the file where you want to save the data
     */
    private static void saveOnDisk(Iterable<Entry<MemorySegment>> iterable, Path tablePath) throws IOException {
        try (Arena writeArena = Arena.ofConfined()) {
            long ssTableSize = 0;
            long entriesCount = 0;
            Iterator<Entry<MemorySegment>> iterator = iterable.iterator();
            Entry<MemorySegment> entry;
            while (iterator.hasNext()) {
                entry = iterator.next();
                long valueSize = entry.value() == null ? 0 : entry.value().byteSize();
                ssTableSize += Long.BYTES + entry.key().byteSize() + Long.BYTES + valueSize;
                entriesCount++;
            }
            long entriesStartIndex = getOffsetInBytes(entriesCount);

            MemorySegment mappedSsTableFile = mapFile(tablePath, ssTableSize + entriesStartIndex,
                FileChannel.MapMode.READ_WRITE, writeArena,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

            long indexOffset = Long.BYTES;
            long dataOffset = 0;
            mappedSsTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, entriesCount);
            dataOffset += entriesStartIndex;
            iterator = iterable.iterator();
            while (iterator.hasNext()) {
                entry = iterator.next();
                mappedSsTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                indexOffset += Long.BYTES;
                dataOffset += writeSegmentToMappedTableFile(mappedSsTableFile, entry.key(), dataOffset);
                dataOffset += writeSegmentToMappedTableFile(mappedSsTableFile, entry.value(), dataOffset);
            }
        }
    }

    private static MemorySegment mapFile(Path filePath, long bytesSize, FileChannel.MapMode mapMode, Arena arena,
                                         OpenOption... options) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(filePath, options)) {
            return fileChannel.map(mapMode, 0, bytesSize, arena);
        }
    }

    private static long writeSegmentToMappedTableFile(MemorySegment mappedTableFile,
                                                      MemorySegment segment, long offset) {
        if (segment == null) {
            mappedTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, NULL_SIZE);
            return Long.BYTES;
        }
        mappedTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, segment.byteSize());
        MemorySegment.copy(segment, 0, mappedTableFile, offset + Long.BYTES, segment.byteSize());
        return Long.BYTES + segment.byteSize();
    }

    private static Entry<MemorySegment> getEntryByIndex(MemorySegment mappedSsTable, long index) {
        long entryOffset = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, getOffsetInBytes(index));
        long keySize = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
        long valueSize = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset + Long.BYTES + keySize);
        return new BaseEntry<>(
            mappedSsTable.asSlice(entryOffset + Long.BYTES, keySize),
            valueSize == NULL_SIZE ? null :
                mappedSsTable.asSlice(entryOffset + Long.BYTES + keySize + Long.BYTES, valueSize)
        );
    }

    private static long getOffsetInBytes(long index) {
        return Long.BYTES + Long.BYTES * index;
    }

    private long binarySearchIndex(MemorySegment ssTable, MemorySegment key) {
        long entriesCount = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        long left = 0;
        long right = entriesCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;
            long keyOffset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, getOffsetInBytes(mid));
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
            keyOffset += Long.BYTES;

            long mismatchResult = MemorySegment.mismatch(ssTable, keyOffset, keyOffset + keySize,
                key, 0, key.byteSize());
            if (mismatchResult == -1) {
                return mid;
            }
            if (mismatchResult == keySize) {
                left = mid + 1;
                continue;
            }
            if (mismatchResult == key.byteSize()) {
                right = mid - 1;
                continue;
            }

            int result = Byte.compare(ssTable.get(ValueLayout.JAVA_BYTE, keyOffset + mismatchResult),
                key.get(ValueLayout.JAVA_BYTE, mismatchResult));
            if (result < 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return left;
    }

    private Path getTablePathForIndex(int index) {
        String tableIndex = String.format("%010d", index);
        return config.basePath().resolve(TABLE_FILENAME + tableIndex + TABLE_EXTENSION);
    }
}
