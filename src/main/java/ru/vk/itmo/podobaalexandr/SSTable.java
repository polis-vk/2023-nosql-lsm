package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;

public class SSTable {

    private static final String FILE_NAME = "text.txt";

    private final Path filePath;

    private Arena arena;

    private final MemorySegment page;

    private long offsetK = 0;

    private long offsetV = 0;

    private final Comparator<MemorySegment> comparatorMem;

    public SSTable(Path path, Comparator<MemorySegment> comparator) {
        filePath = path;
        comparatorMem = comparator;

        arena = Arena.ofShared();

        MemorySegment pageCurrent;

        boolean created = false;
        try (FileChannel fileChannel = FileChannel.open(path.resolve(FILE_NAME), StandardOpenOption.READ)) {
            long size = Files.size(path.resolve(FILE_NAME));
            pageCurrent = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
            created = true;
        } catch (FileNotFoundException | NoSuchFileException e) {
            pageCurrent = null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (!created) {
                arena.close();
                arena = null;
            }
        }

        page = pageCurrent;

    }

    public Entry<MemorySegment> get(MemorySegment keySearch) {

        if (page == null) {
            return null;
        }

        long offset = Long.BYTES;
        Entry<MemorySegment> res = null;
        long keysSize = page.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        while (offset < keysSize) {
            long keySize = page.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
            offset += Long.BYTES;

            MemorySegment key = page.asSlice(offset, keySize);
            offset += keySize;

            int compare = comparatorMem.compare(keySearch, key);
            if (compare == 0) {
                long offsetToV = page.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
                offset += 2 * Long.BYTES;

                MemorySegment value = null;
                if (offset < keysSize) {
                    keySize = page.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
                    offset += keySize + Long.BYTES;

                    long size = page.get(ValueLayout.JAVA_INT_UNALIGNED, offset) - offsetToV;

                    value = page.asSlice(offsetToV, size);
                }

                value = value == null ? page.asSlice(offsetToV) : value;

                res = new BaseEntry<>(key, value);
                offset = page.byteSize();
            } else if (compare > 0) {
                offset += 2 * Long.BYTES;
            } else {
                offset += Long.BYTES;
                offset = page.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
                if (offset == -1) {
                    return null;
                }
            }
        }
        return res;
    }

    public void save(Collection<Entry<MemorySegment>> entries) {

        if (arena != null) {
            if (!arena.scope().isAlive()) {
                return;
            }

            arena.close();
        }

        for (Entry<MemorySegment> entry : entries) {
            offsetK += entry.key().byteSize() + 3 * Long.BYTES;
            offsetV += entry.value().byteSize();
        }

        offsetK += Long.BYTES;
        offsetV += offsetK;



        try {
            if (!Files.exists(filePath)) {
                Files.createDirectory(filePath);
            }
            sureSave(entries);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void sureSave(Collection<Entry<MemorySegment>> entries) {
        OpenOption[] options = {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

        try (Arena arenaWrite = Arena.ofConfined();
             FileChannel fileChannel = FileChannel.open(filePath.resolve(FILE_NAME), options)) {

            MemorySegment fileSegment = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, offsetV, arenaWrite);

            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, offsetK);
            log2Save(0, entries.size() - 1, fileSegment, entries.iterator());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public long log2Save(int lo, int hi, MemorySegment fileSegment,
                         Iterator<Entry<MemorySegment>> iterator) {
        if (hi < lo) {
            return -1;
        }

        int mid = (lo + hi) >>> 1;

        long offsetL = 0L;

        if (lo < mid) {
            offsetL = log2Save(lo, mid - 1, fileSegment, iterator);
        }

        Entry<MemorySegment> entry = iterator.next();
        MemorySegment key = entry.key();
        MemorySegment value = entry.value();

        if (mid < hi) {
            log2Save(mid + 1, hi, fileSegment, iterator);
        }

        offsetV -= value.byteSize();
        MemorySegment.copy(value, 0, fileSegment, offsetV, value.byteSize());

        offsetK -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetK, offsetL);

        offsetK -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetK, offsetV);

        offsetK -= key.byteSize();
        MemorySegment.copy(key, 0, fileSegment, offsetK, key.byteSize());

        offsetK -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetK, key.byteSize());

        return offsetK;
    }

}
