package ru.vk.itmo.at;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import ru.vk.itmo.BaseEntry;

public class SSTable implements ISSTable {
    private final MemorySegmentComparator comparator;
    private final Arena arena;
    private final MemorySegment sstable;

    public static ISSTable load(String sstableName, MemorySegmentComparator comparator) throws IOException {
        if (Files.exists(Paths.get(sstableName))) {
            try (RandomAccessFile file = new RandomAccessFile(sstableName, "r")) {
                Arena arena = Arena.ofShared();
                MemorySegment st = file.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length(), arena);
                return new SSTable(st, comparator, arena);
            }
        } else {
            return SSTableEmpty.INSTANCE;
        }
    }

    public static void save(String sstableName, Collection<BaseEntry<MemorySegment>> entries) throws IOException {
        long size = 0;
        for (BaseEntry<MemorySegment> e : entries) {
            MemorySegment key = e.key();
            MemorySegment value = e.value();
            size += key.byteSize();
            size += value.byteSize();
            size += ValueLayout.OfByte.JAVA_LONG_UNALIGNED.byteSize();
            size += ValueLayout.OfByte.JAVA_LONG_UNALIGNED.byteSize();
        }

        try (RandomAccessFile file = new RandomAccessFile(sstableName, "rw"); Arena arena = Arena.ofConfined()) {
            MemorySegment st = file.getChannel().truncate(size).map(FileChannel.MapMode.READ_WRITE, 0, size, arena);
            long offset = 0;
            for (BaseEntry<MemorySegment> e : entries) {
                MemorySegment key = e.key();
                MemorySegment value = e.value();
                long keySize = key.byteSize();
                long valueSize = value.byteSize();
                st.set(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset, keySize);
                offset += ValueLayout.OfByte.JAVA_LONG_UNALIGNED.byteSize();
                st.set(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset, valueSize);
                offset += ValueLayout.OfByte.JAVA_LONG_UNALIGNED.byteSize();
                MemorySegment.copy(key, 0, st, offset, keySize);
                offset += keySize;
                MemorySegment.copy(value, 0, st, offset, valueSize);
                offset += valueSize;
            }
        }
    }

    private SSTable(MemorySegment sstable, MemorySegmentComparator comparator, Arena arena) {
        this.sstable = sstable;
        this.comparator = comparator;
        this.arena = arena;
    }

    @Override
    public BaseEntry<MemorySegment> get(MemorySegment key) {
        long offset = 0;
        while (offset < sstable.byteSize()) {
            long keySize = sstable.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
            offset += ValueLayout.OfByte.JAVA_LONG_UNALIGNED.byteSize();
            long valueSize = sstable.get(ValueLayout.OfByte.JAVA_LONG_UNALIGNED, offset);
            offset += ValueLayout.OfByte.JAVA_LONG_UNALIGNED.byteSize();
            MemorySegment k = sstable.asSlice(offset, keySize);
            offset += keySize;
            MemorySegment v = sstable.asSlice(offset, valueSize);
            offset += valueSize;

            if (comparator.compare(k, key) == 0) {
                return new BaseEntry<>(k, v);
            }
        }
        return null;
    }

    @Override
    public void close() {
        arena.close();
    }
}
