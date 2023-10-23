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
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Storage {

    private static final String TABLE_FILENAME = "ssTable";
    private static final String COMPACTED_TABLE_FILENAME = TABLE_FILENAME + "Compact";
    private static final String TABLE_EXTENSION = ".dat";

    private final Arena arena = Arena.ofShared();
    private final Config config;
    private final List<MemorySegment> mappedSsTables;
    private final MemorySegmentComparator memorySegmentComparator;

    /*
    Filling ssTable with bytes from the memory segment with a structure:
    [entry_count]{[entry_pos]...}{[key_size][key][value_size][value]}...

    If value is null then value_size = -1
    */
    public Storage(Config config) {
        this.config = config;
        this.memorySegmentComparator = new MemorySegmentComparator();
        mappedSsTables = new ArrayList<>();
        Path tablesDir = config.basePath();

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
                        throw new ApplicationException("Can't access the file", e);
                    }
                });
        } catch (NoSuchFileException e) {
            Logger logger = Logger.getLogger(this.getClass().getPackage().getName());
            logger.warning("Can't find the file");
        } catch (IOException e) {
            throw new ApplicationException("Can't access the file", e);
        }
        mappedSsTables.reversed();
    }

    public Iterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(mappedSsTables.size());
        for (MemorySegment mappedSsTable: mappedSsTables) {
            long fromPos;
            long toPos;
            if (from == null) {
                fromPos = 0;
            } else {
                fromPos = binarySearchIndex(mappedSsTable, from);
            }
            if (to == null) {
                toPos = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
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

        if (entries.isEmpty()) {
            return;
        }

        String tableIndex = String.format("%010d", mappedSsTables.size());
        Path tablePath = config.basePath().resolve(TABLE_FILENAME + tableIndex + TABLE_EXTENSION);
        saveOnDisk(entries, tablePath);
    }

    public void compact(Iterable<Entry<MemorySegment>> iterable) throws IOException {
        if (!arena.scope().isAlive() || !iterable.iterator().hasNext()) {
            return;
        }

        Path compactedTablePath = config.basePath().resolve(COMPACTED_TABLE_FILENAME + TABLE_EXTENSION);
        saveOnDisk(iterable, compactedTablePath);
        arena.close();

        try (Stream<Path> files = Files.list(config.basePath())) {
            Pattern pattern = Pattern.compile(TABLE_FILENAME + "\\d*" + TABLE_EXTENSION + "$");
            files
                .filter(path -> pattern.matcher(path.toString()).find())
                .forEach(tablePath -> {
                    try {
                        Files.delete(tablePath);
                    } catch (IOException e) {
                        throw new ApplicationException("Can't delete file", e);
                    }
                });
        }

        String tableIndex = String.format("%010d", 0);
        Path tablePath = config.basePath().resolve(TABLE_FILENAME + tableIndex + TABLE_EXTENSION);
        Files.move(compactedTablePath, tablePath);
    }

    /*
    Saving to disk is done using Iterable. Iterable allows you to return multiple iterators.
    In our case, the first iterator is used to calculate the size of the resulting ssTable and the number of entries,
    and the second one is used to write segments directly to the mapped ssTable.

    In addition, using Iterable allows you to make the saveOnDisk() method universal,
    since saving is performed in two cases: when calling close(), that is, all entries from NavigableMap are written,
    and also when compact(), which involves the use of iterators.
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
            long entriesStartIndex = Long.BYTES + Long.BYTES * entriesCount;

            MemorySegment mappedSsTableFile = mapFile(tablePath, ssTableSize + entriesStartIndex,
                FileChannel.MapMode.READ_WRITE, writeArena,
                StandardOpenOption.READ, StandardOpenOption.WRITE,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            long offset = 0;
            long index = 0;
            mappedSsTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entriesCount);
            offset += Long.BYTES + Long.BYTES * entriesCount;
            iterator = iterable.iterator();
            while (iterator.hasNext()) {
                entry = iterator.next();
                mappedSsTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + Long.BYTES * index, offset);
                index++;
                offset += writeSegmentToMappedTableFile(mappedSsTableFile, entry.key(), offset);
                offset += writeSegmentToMappedTableFile(mappedSsTableFile, entry.value(), offset);
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
            mappedTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
            return Long.BYTES;
        }
        mappedTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, segment.byteSize());
        MemorySegment.copy(segment, 0, mappedTableFile, offset + Long.BYTES, segment.byteSize());
        return Long.BYTES + segment.byteSize();
    }

    private static Entry<MemorySegment> getEntryByIndex(MemorySegment mappedSsTable, long index) {
        long entryPos = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + Long.BYTES * index);
        long keySize = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, entryPos);
        long valueSize = mappedSsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, entryPos + Long.BYTES + keySize);
        return new BaseEntry<>(
            mappedSsTable.asSlice(entryPos + Long.BYTES, keySize),
            valueSize == -1 ? null : mappedSsTable.asSlice(entryPos + Long.BYTES + keySize + Long.BYTES, valueSize)
        );
    }

    private long binarySearchIndex(MemorySegment ssTable, MemorySegment key) {
        long entriesCount = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        long left = 0;
        long right = entriesCount - 1;
        while (left <= right) {
            long mid = (left + right) >>> 1;
            long keyPos = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + Long.BYTES * mid);
            long keySize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyPos);

            MemorySegment keyFromTable = ssTable.asSlice(keyPos + Long.BYTES, keySize);
            int result = memorySegmentComparator.compare(keyFromTable, key);
            if (result < 0) {
                left = mid + 1;
            } else if (result > 0) {
                right = mid - 1;
            } else {
                return mid;
            }
        }
        return left;
    }
}
