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

public class BinarySearchSSTable {
    private long tableSize;
    private long indexSize;
    private final MemorySegment tableSegment;
    private final MemorySegment indexSegment;
    public int id;

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

    BinarySearchSSTable(Path path, Arena arena) {
        this.id = Integer.parseInt(path.getFileName().toString().substring(8));
        Path indexPath = Paths.get(path.toAbsolutePath().toString() + "_index");

        try {
            if (Files.exists(path)) {
                this.tableSize = Files.size(path);
            }
            if (Files.exists(indexPath)) {
                this.indexSize = Files.size(indexPath);
            }
        } catch (IOException e) {
            throw new SSTableCreationException(e);
        }
        try (FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.READ)) {
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

    public static Path WriteSSTable(Collection<Entry<MemorySegment>> entries, Path path, int id) {
        Arena arena = Arena.ofConfined();
        Path sstPath = Path.of(path.toAbsolutePath() + "/sstable_" + id);
        Path sstIndexPath = Path.of(path.toAbsolutePath() + "/sstable_" + id + "_index");
        MemorySegment tableSegment, indexSegment;
        long dataSize = 0;
        long indexSize = 0;
        for (var entry : entries) {
            dataSize += entry.value().byteSize() + entry.key().byteSize();
            indexSize += 16; //todo: заменить по коментам из ПР
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
        tableSegment.load();
        indexSegment.load();
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
            indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, tableOffset);
            indexOffset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
            MemorySegment.copy(entry.value(), 0, tableSegment, tableOffset, entry.value().byteSize());
            tableOffset += entry.value().byteSize();
        }
    }

    private long searchEntryPosition(MemorySegment key, boolean exact) {
        long l = 0, r = this.indexSize / 16 - 1;
        long m;
        while (l <= r) {
            m = l + (r - l) / 2;

            var keyOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m * 16);
            var valOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m * 16 + 8);

            var mismatch = MemorySegment.mismatch(key, 0, key.byteSize(), tableSegment, keyOffset, valOffset);
            if (mismatch == -1) {
                return m;
            } else {
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
                mismatch = Byte.compare(b1, b2);
                if (mismatch < 0) {
                    r = m - 1;
                } else {
                    l = m + 1;
                }
            }
        }
        return exact ? -1 : l;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        MemorySegment val;
        var m = this.searchEntryPosition(key, true);
        if (m == -1) return null;
        var valOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m * 16 + 8);

        if ((m + 1) * 16 == indexSize) {
            // Случай когда мы не можем посчитать размер значения тк не имеем оффсета следующего за ним элемента
            val = tableSegment.asSlice(valOffset, tableSize - valOffset);
        } else {
            var nextOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, (m + 1) * 16);
            val = tableSegment.asSlice(valOffset, nextOffset - valOffset);
        }
        return new BaseEntry<>(key, val);

    }

    public Iterator<Entry<MemorySegment>> scan(MemorySegment keyFrom, MemorySegment keyTo) {
        long startIndex;
        long endIndex;
        if (keyFrom == null) {
            startIndex = 0;
        } else {
            startIndex = this.searchEntryPosition(keyFrom, false);
        }
        if (keyTo == null) {
            endIndex = this.indexSize / 16;
        } else {
            endIndex = this.searchEntryPosition(keyTo, false);
        }
        return new BinarySearchSSTableIterator(
                this.indexSegment,
                this.tableSegment,
                startIndex,
                endIndex
        );
    }
}

class BinarySearchSSTableIterator implements Iterator<Entry<MemorySegment>> {
    private final long endEntryIndex;
    private long currentEntryIndex;

    private final MemorySegment indexSegment;
    private final MemorySegment tableSegment;

    public BinarySearchSSTableIterator(
            MemorySegment indexSegment,
            MemorySegment tableSegment,
            long startEntryIndex,
            long endEntryIndex
    ) {
        this.currentEntryIndex = startEntryIndex;
        this.endEntryIndex = endEntryIndex;
        this.indexSegment = indexSegment;
        this.tableSegment = tableSegment;
    }

    @Override
    public boolean hasNext() {
        return this.currentEntryIndex != this.endEntryIndex;
    }


    @Override
    public Entry<MemorySegment> next() {
        var keyOffset = this.indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, this.currentEntryIndex * 16);
        var valOffset = this.indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, this.currentEntryIndex * 16 + 8);//TODO заменить по коменту из ПР
        long nextOffset;
        if (this.currentEntryIndex * 16 + 16 >= this.indexSegment.byteSize()) {
            nextOffset = this.tableSegment.byteSize();
        } else {
            nextOffset = this.indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, this.currentEntryIndex * 16 + 16);
        }
        this.currentEntryIndex++;
        return new BaseEntry<>(
                this.tableSegment.asSlice(keyOffset, valOffset - keyOffset),
                this.tableSegment.asSlice(valOffset, nextOffset - valOffset)
        );
    }
}
