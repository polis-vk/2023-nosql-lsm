package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;

public class BinarySearchSSTable implements SSTable<MemorySegment, Entry<MemorySegment>> {
    private long tableSize;
    private long indexSize;
    private final MemorySegment tableSegment;
    private final MemorySegment indexSegment;
    public int id;
    public boolean closed;
    public final Path tablePath;
    public final Path indexPath;
    private final Arena arena;

    private static class SSTableCreationException extends RuntimeException {
        public SSTableCreationException(Throwable cause) {
            super(cause);
        }
    }

    private static class SSTableRWException extends RuntimeException {
        public SSTableRWException(Throwable cause) {
            super(cause);
        }
    }

    private static class ClosedSSTableAccess extends RuntimeException {
        public ClosedSSTableAccess() {
            super();
        }
    }

    BinarySearchSSTable(Path path, Arena arena) {
        this.closed = false;
        this.arena = arena;
        this.id = Integer.parseInt(path.getFileName().toString().substring(8));
        tablePath = path;
        indexPath = Paths.get(path.toAbsolutePath() + "_index");

        try {
            if (Files.exists(tablePath)) {
                this.tableSize = Files.size(tablePath);
            }
            if (Files.exists(indexPath)) {
                this.indexSize = Files.size(indexPath);
            }
        } catch (IOException e) {
            throw new SSTableCreationException(e);
        }
        try (FileChannel fileChannel = FileChannel.open(tablePath, StandardOpenOption.READ)) {
            this.tableSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, tableSize, arena);
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }
        try (FileChannel fileChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            this.indexSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexSize, arena);
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }
    }

    public static Path writeSSTable(Collection<Entry<MemorySegment>> entries, Path path, int id) {
        Arena arena = Arena.ofConfined();
        Path sstPath = Path.of(path.toAbsolutePath() + "/sstable_" + id);
        Path sstIndexPath = Path.of(path.toAbsolutePath() + "/sstable_" + id + "_index");
        MemorySegment tableSegment;
        MemorySegment indexSegment;
        long dataSize = 0;
        long indexSize = 0;
        for (var entry : entries) {
            dataSize += entry.key().byteSize();

            if (entry.value() != null) {
                dataSize += entry.value().byteSize();
            }
            indexSize += Long.BYTES * 2;
        }
        try (var fileChannel = FileChannel.open(
                sstPath,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            tableSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataSize, arena);
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }

        try (var fileChannel = FileChannel.open(
                sstIndexPath,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
        )) {
            indexSegment = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize, arena);
        } catch (IOException e) {
            throw new SSTableRWException(e);
        }
        writeEntries(entries, tableSegment, indexSegment);
        arena.close();
        return sstPath;
    }

    private static void writeEntries(
            Collection<Entry<MemorySegment>> entries,
            MemorySegment tableSegment,
            MemorySegment indexSegment
    ) {
        long tableOffset = 0;
        long indexOffset = 0;
        for (var entry : entries) {
            indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tableOffset);
            indexOffset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

            MemorySegment.copy(entry.key(), 0, tableSegment, tableOffset, entry.key().byteSize());
            tableOffset += entry.key().byteSize();
            if (entry.value() == null) {
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tombstone(tableOffset));
                indexOffset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                continue;
            }
            indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tableOffset);
            indexOffset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

            MemorySegment.copy(entry.value(), 0, tableSegment, tableOffset, entry.value().byteSize());
            tableOffset += entry.value().byteSize();
        }
    }

    private long searchEntryPosition(MemorySegment key, boolean exact) {
        long l = 0;
        long r = this.indexSize / (Long.BYTES * 2) - 1;
        long m;
        while (l <= r) {
            m = l + (r - l) / 2;

            long keyOffset = getKeyOffset(m);
            long valOffset = normalize(getValOffset(m));
            long mismatch = MemorySegment.mismatch(key, 0, key.byteSize(), tableSegment, keyOffset, valOffset);
            if (mismatch == -1) {
                return m;
            }
            if (mismatch == valOffset - keyOffset) {
                l = m + 1;
                continue;
            }
            if (mismatch == key.byteSize()) {
                r = m - 1;
                continue;
            }
            byte b1 = key.get(ValueLayout.JAVA_BYTE, mismatch);
            byte b2 = tableSegment.get(ValueLayout.JAVA_BYTE, keyOffset + mismatch);
            int keysBytesCompared = Byte.compare(b1, b2);
            if (keysBytesCompared < 0) {
                r = m - 1;
            } else {
                l = m + 1;
            }
        }
        return exact ? -1 : l;
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        MemorySegment val;
        long m = this.searchEntryPosition(key, true);
        if (m == -1) return null;
        long valOffset = getValOffset(m);
        long recordEnd = getRecordEnd(m);
        val = valOffset < 0 ? null : tableSegment.asSlice(valOffset, recordEnd - valOffset);
        return new BaseEntry<>(key, val);
    }

    @Override
    public Iterator<Entry<MemorySegment>> scan(MemorySegment keyFrom, MemorySegment keyTo) {
        long startIndex;
        long endIndex;
        if (keyFrom == null) {
            startIndex = 0;
        } else {
            startIndex = this.searchEntryPosition(keyFrom, false);
        }
        if (keyTo == null) {
            endIndex = this.indexSize / (Long.BYTES * 2);
        } else {
            endIndex = this.searchEntryPosition(keyTo, false);
        }
        return iterator(
                startIndex,
                endIndex
        );
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(long value) {
        return value & ~(1L << 63);
    }

    private long getKeyOffset(long recordIndex) {
        return indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * Long.BYTES * 2);
    }

    private long getValOffset(long recordIndex) {
        return indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, recordIndex * Long.BYTES * 2 + Long.BYTES);
    }

    private long getRecordEnd(long recordIndex) {
        if ((recordIndex + 1) * Long.BYTES * 2 == indexSize) {
            // Случай когда мы не можем посчитать размер значения тк не имеем оффсета следующего за ним элемента
            return tableSize;
        } else {
            return indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (recordIndex + 1) * Long.BYTES * 2);
        }
    }

    private Iterator<Entry<MemorySegment>> iterator(long startEntryIndex, long endEntryIndex) {
        return new Iterator<>() {
            long currentEntryIndex = startEntryIndex;

            @Override
            public boolean hasNext() {
                return this.currentEntryIndex != endEntryIndex;
            }

            @Override
            public Entry<MemorySegment> next() {
                var keyOffset = getKeyOffset(currentEntryIndex);
                var valOffset = getValOffset(currentEntryIndex);
                long nextOffset = getRecordEnd(currentEntryIndex);
                this.currentEntryIndex++;
                return new BaseEntry<>(
                        tableSegment.asSlice(keyOffset, normalize(valOffset) - keyOffset),
                        valOffset < 0
                                ?
                                null
                                :
                                tableSegment.asSlice(
                                        normalize(valOffset),
                                        nextOffset - normalize(valOffset)
                                )
                );
            }
        };
    }

    public void close() {
        if (closed) throw new ClosedSSTableAccess();
        arena.close();
        closed = true;
    }
}
