package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

public class PersistentDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {

    private static final AtomicInteger ssTableId = new AtomicInteger(0);

    private static final String DATA_FILE = "data.db";

    private static final String INDEX_FILE = "index.db";
    private static final Set<StandardOpenOption> WRITE_OPTIONS = Set.of(
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
    );
    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable
            = new ConcurrentSkipListMap<>(MemorySegmentComparator.getInstance());
    private final CopyOnWriteArraySet<SStable> ssTables = new CopyOnWriteArraySet<>();
    private final Path sourceDirectory;
    private final Arena arena;

    public PersistentDaoImpl(Config config) throws IOException {
        sourceDirectory = config.basePath();
        if (!Files.exists(sourceDirectory)) {
            Files.createDirectory(sourceDirectory);
        }
        arena = Arena.ofShared();

        FileVisitor<Path> visitor = new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (attrs.isDirectory() && isRequiredDirectory(file)) {
                    ssTables.add(new SStable(file, arena));
                }
                return FileVisitResult.CONTINUE;
            }

            private boolean isRequiredDirectory(Path file) {
                return file.getFileName().toString().startsWith("sstable");
            }
        };
        Files.walkFileTree(sourceDirectory, Set.of(), 1, visitor);
    }


    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<SSTablePeekableIterator> iterators = new ArrayList<>();
        for (SStable sStable : ssTables) {
            iterators.add(sStable.iterator(from, to));
        }
        return new MergeIterator(getPeekableIterator(from, to), iterators);
    }

    private Iterator<Entry<MemorySegment>> getFromMemTable(MemorySegment from, MemorySegment to) {
        if (from == null) {
            if (to != null) {
                return memTable.headMap(to).values().iterator();
            }
            return memTable.values().iterator();
        } else {
            if (to == null) {
                return memTable.tailMap(from).values().iterator();
            }
            return memTable.subMap(from, true, to, false).values().iterator();
        }
    }

    private MemTablePeekableIterator getPeekableIterator(MemorySegment from, MemorySegment to) {
        return new MemTablePeekableIterator(getFromMemTable(from, to));
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        Entry<MemorySegment> value = memTable.get(key);
        if (value != null) {
            return value.value() == null ? null : value;
        }
        long changeTimestamp = 0;
        for (SStable ssTable : ssTables) {
            var val = ssTable.getData(key);
            if (val != null) {
                if (val.timestamp() > changeTimestamp) {
                    if (val.entry().value() == null) {
                        value = null;
                    } else {
                        value = val.entry();
                    }
                    changeTimestamp = val.timestamp();
                }
            }
        }
        return value;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void flush() throws IOException {
        Path ssTableDir = Files.createDirectory(sourceDirectory.resolve("sstable" + ssTableId.incrementAndGet()));

        try (FileChannel dataChannel = FileChannel.open(ssTableDir.resolve(DATA_FILE), WRITE_OPTIONS);
             FileChannel indexChanel = FileChannel.open(ssTableDir.resolve(INDEX_FILE), WRITE_OPTIONS)) {
            try (Arena confinedArena = Arena.ofConfined()) {
                MemorySegment indexMemorySegment = indexChanel.map(MapMode.READ_WRITE, 0,
                        calculateIndexFileSize(), confinedArena);
                indexMemorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, 0, memTable.size());
                long indexOffset = Integer.BYTES;


                MemorySegment dataMemorySegment = dataChannel.map(MapMode.READ_WRITE, 0,
                        getMemTableSizeInBytes(), confinedArena);
                long dataOffset = 0;
                for (var entry : memTable.values()) {
                    indexMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                    indexOffset += Long.BYTES;
                    dataOffset = writeEntry(entry, dataMemorySegment, dataOffset);
                }
            }
        }
    }

    private long getMemTableSizeInBytes() {
        long size = memTable.size() * Long.BYTES * 3L;
        for (var entry : memTable.values()) {
            size += entry.key().byteSize() + nullSafetyByteSize(entry.value());
        }
        return size;
    }

    private long nullSafetyByteSize(MemorySegment ms) {
        return ms == null ? 0 : ms.byteSize();
    }

    private long calculateIndexFileSize() {
        return ((long) memTable.size() + 1) * Long.BYTES;
    }

    //[key length][key][value length][value][timestamp of operation]
    private long writeEntry(Entry<MemorySegment> entry, MemorySegment destination, long offset) {
        long currentOffset = writeDataToMemorySegment(entry.key(), destination, offset);
        currentOffset = writeDataToMemorySegment(entry.value(), destination, currentOffset);
        destination.set(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset, System.currentTimeMillis());
        return currentOffset + Long.BYTES;
    }

    private long writeDataToMemorySegment(MemorySegment entryPart, MemorySegment destination, long offset) {
        long currentOffset = offset;

        destination.set(ValueLayout.JAVA_LONG_UNALIGNED, currentOffset, entryPart == null ? -1 : entryPart.byteSize());
        currentOffset += Long.BYTES;

        if (entryPart != null) {
            MemorySegment.copy(entryPart, 0, destination, currentOffset, entryPart.byteSize());
            currentOffset += entryPart.byteSize();
        }
        return currentOffset;
    }

    @Override
    public void close() throws IOException {
        flush();
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();
    }

}
