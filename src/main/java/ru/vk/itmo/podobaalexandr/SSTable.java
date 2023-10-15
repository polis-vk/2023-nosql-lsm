package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Stream;

public class SSTable {

    private final StringBuilder fileName = new StringBuilder("database_");
    private final List<MemorySegment> pages = new ArrayList<>();
    private final Path filePath;
    private long offsetV = 0;
    private Arena arena;

    public SSTable(Path path) {
        filePath = path;
        arena = Arena.ofShared();

        boolean created = false;
        int filesCount = 0;

        if(Files.exists(filePath)) {
            try (Stream<Path> stream = Files.list(filePath).sorted()) {
                List<Path> files = stream.toList();
                filesCount = files.size();
                for (Path file : files) {
                    try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
                        long size = Files.size(file);
                        MemorySegment pageCurrent = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
                        created = true;
                        pages.addFirst(pageCurrent);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                if (!created) {
                    arena.close();
                    arena = null;
                }
            }
        }

        fileName.append(String.format("%010d", filesCount));
    }

    public Entry<MemorySegment> get(MemorySegment keySearch) {

        Entry<MemorySegment> res = null;

        for(MemorySegment page : pages) {
            res = getFromPage(keySearch, page);
            if (res != null) {
                if (res.value() == null) {
                    return null;
                }
                break;
            }
        }

        return res;
    }

    private int compareSegments(MemorySegment src, long srcToOffset,
                                MemorySegment dest, long destFromOffset, long destToOffset) {

        long destLength = destToOffset - destFromOffset;

        int sizeDiff = Long.compare(srcToOffset, destLength);

        if (srcToOffset == 0 || destLength == 0) {
            return sizeDiff;
        }

        long mismatch = MemorySegment.mismatch(src, 0, srcToOffset, dest, destFromOffset, destToOffset);

        if (mismatch == destLength || mismatch == srcToOffset) {
            return sizeDiff;
        }

        return mismatch == -1
                ? 0
                : Byte.compare(src.get(ValueLayout.JAVA_BYTE, mismatch),
                dest.get(ValueLayout.JAVA_BYTE, destFromOffset + mismatch));
    }

    private Entry<MemorySegment> getKeyValueFromOffset(MemorySegment page, long offset, long keySize, long keysSize) {
        MemorySegment key = page.asSlice(offset, keySize);
        offset += keySize;

        long offsetToV = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        if (offsetToV == 0) {
            return new BaseEntry<>(key, null);
        }

        offset += 2 * Long.BYTES + Byte.BYTES;
        MemorySegment value = null;
        long size = 0;
        while (size <= 0 && offset < keysSize) {
            keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES + keySize;

            size = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset) - offsetToV;
            if (size > 0) {
                value = page.asSlice(offsetToV, size);
            }
            offset += 2 * Long.BYTES + Byte.BYTES;
        }

        value = value == null ? page.asSlice(offsetToV) : value;

        return new BaseEntry<>(key, value);
    }

    public Collection<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to,
                                                ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> mapMemory) {

        if (arena == null) {
            return mapMemory.values();
        }

        TreeMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(mapMemory);

        if (from != null && to != null) {
            allPagesFromTo(entries, from, to);
        } else if (from == null && to == null) {
            allPages(entries);
        } else if (from != null) {
            allPagesFrom(entries, from);
        } else {
            allPagesTo(entries, to);
        }

        return entries.values();
    }

    private void allPagesFromTo(TreeMap<MemorySegment, Entry<MemorySegment>> entries,
                                MemorySegment from, MemorySegment to) {
        if (compareSegments(from, from.byteSize(), to, 0, to.byteSize()) < 0) {
            for (MemorySegment page : pages) {
                long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
                allPageFromTo(page, entries, Long.BYTES, keysSize, from, to);
            }
        }
    }

    private void allPagesTo(TreeMap<MemorySegment, Entry<MemorySegment>> entries, MemorySegment to) {
        for (MemorySegment page: pages) {
            long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            allPageTo(page, entries, Long.BYTES, keysSize, to);
        }
    }

    private void allPagesFrom(TreeMap<MemorySegment, Entry<MemorySegment>> entries, MemorySegment from) {
        for (MemorySegment page: pages) {
            long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            allPageFrom(page, entries, Long.BYTES, keysSize, from);
        }
    }

    private void allPages(TreeMap<MemorySegment, Entry<MemorySegment>> entries) {
        for (MemorySegment page: pages) {
            long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            allPage(page, entries, Long.BYTES, keysSize);
        }
    }

    //Invariant: FROM < TO
    private void allPageFromTo(MemorySegment page, TreeMap<MemorySegment, Entry<MemorySegment>> entries,
                               long offset, long keysSize, MemorySegment from, MemorySegment to) {
        if (keysSize <= offset || offset == 0) {
            return;
        }

        Entry<MemorySegment> last;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        long compareFrom = compareSegments(from, from.byteSize(), page, offset, offset + keySize);
        long compareTo = compareSegments(to, to.byteSize(), page, offset, offset + keySize);

        if (compareFrom < 0 && compareTo > 0) {
            last = getKeyValueFromOffset(page, offset, keySize, keysSize);
            offset += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            allPageFrom(page, entries, offsetToL, keysSize, from);
            offset += Long.BYTES;

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offset);
            long rightOffset = isRightHere == 1 ? offset + Byte.BYTES : 0;
            allPageTo(page, entries, rightOffset, keysSize, to);
        } else if (compareFrom == 0) {
            last = getKeyValueFromOffset(page, offset, keySize, keysSize);
            offset += keySize + 2 * Long.BYTES;

            entries.putIfAbsent(last.key(), last);
            long rightOffset = page.get(ValueLayout.JAVA_BYTE, offset) == 1 ? offset + Byte.BYTES : 0;
                allPageTo(page, entries, rightOffset, keysSize, to);
        } else if (compareTo == 0) {
            offset += keySize + Long.BYTES;
            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            allPageFrom(page, entries, offsetToL, keysSize, from);
        } else if (compareFrom > 0) {
            offset += keySize + 2 * Long.BYTES;
            long rightOffset = page.get(ValueLayout.JAVA_BYTE, offset) == 1 ? offset + Byte.BYTES : 0;
            allPageFromTo(page, entries, rightOffset, keysSize, from, to);
        } else {
            offset += keySize + Long.BYTES;
            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            allPageFromTo(page, entries, offsetToL, keysSize, from, to);
        }
    }

    private void allPageTo(MemorySegment page, TreeMap<MemorySegment, Entry<MemorySegment>> entries,
                           long offset, long keysSize, MemorySegment to) {
        if (keysSize <= offset || offset == 0) {
            return;
        }

        Entry<MemorySegment> last;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        int compare = compareSegments(to, to.byteSize(), page, offset, offset + keySize);

        if (compare == 0) {
            offset += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            allPage(page, entries, offsetToL, keysSize);
        } else if (compare > 0) {
            last = getKeyValueFromOffset(page, offset, keySize, keysSize);
            offset += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            allPage(page, entries, offsetToL, keysSize);

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offset);
            allPageTo(page, entries, isRightHere == 1 ? offset + Byte.BYTES : 0, keysSize, to);
        } else {
            offset += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            allPageTo(page, entries, offsetToL, keysSize, to);
        }
    }

    private void allPageFrom(MemorySegment page, TreeMap<MemorySegment, Entry<MemorySegment>> entries,
                             long offset, long keysSize, MemorySegment from) {
        if (keysSize <= offset || offset == 0) {
            return;
        }

        Entry<MemorySegment> last;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        int compare = compareSegments(from, from.byteSize(), page, offset, offset + keySize);

        if (compare == 0) {
            last = getKeyValueFromOffset(page, offset, keySize, keysSize);
            offset += 2 * Long.BYTES + keySize;

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offset);
            allPage(page, entries, isRightHere == 1 ? offset + Byte.BYTES : 0, keysSize);
        } else if (compare > 0) {
            offset += keySize + 2 * Long.BYTES;
            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offset);
            allPageFrom(page, entries, isRightHere == 1 ? offset + Byte.BYTES : 0, keysSize, from);
        } else {
            last = getKeyValueFromOffset(page, offset, keySize, keysSize);
            offset += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            allPageFrom(page, entries, offsetToL, keysSize, from);
            offset += Long.BYTES;

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offset);
                allPage(page, entries, isRightHere == 1 ? offset + Byte.BYTES : 0, keysSize);
        }
    }

    private void allPage(MemorySegment page, TreeMap<MemorySegment, Entry<MemorySegment>> entries,
                         long offset, long keysSize) {
        if (keysSize <= offset || offset == 0) {
            return;
        }

        Entry<MemorySegment> last;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;

        last = getKeyValueFromOffset(page, offset, keySize, keysSize);

        offset += keySize + Long.BYTES;
        long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        allPage(page, entries, offsetToL, keysSize);

        entries.putIfAbsent(last.key(), last);

        offset += Long.BYTES;
        byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offset);
        allPage(page, entries, isRightHere == 1 ? offset + Byte.BYTES : 0, keysSize);
    }

    private Entry<MemorySegment> getFromPage(MemorySegment keySearch, MemorySegment page) {
        long offset = Long.BYTES;
        Entry<MemorySegment> res = null;
        long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);

        while (offset < keysSize) {
            long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            int compare = compareSegments(keySearch, keySearch.byteSize(),
                    page, offset, offset + keySize);

            if (compare == 0) {
                res = getKeyValueFromOffset(page, offset, keySize, keysSize);
                break;
            } else if (compare > 0) {
                offset += keySize + 2 * Long.BYTES;
                byte isRightExist = page.get(ValueLayout.JAVA_BYTE, offset);
                if (isRightExist == 0) {
                    return null;
                }
                offset += Byte.BYTES;
            } else {
                offset += keySize + Long.BYTES;
                offset = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                if (offset == 0) {
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

        long offsetK = 0L;

        for (Entry<MemorySegment> entry : entries) {
            offsetK += entry.key().byteSize() + 3 * Long.BYTES + Byte.BYTES;
            offsetV += entry.value() == null ? 0 : entry.value().byteSize();
        }

        offsetK += Long.BYTES;
        offsetV += offsetK;

        try {
            if (!Files.exists(filePath)) {
                Files.createDirectory(filePath);
            }
            if (!entries.isEmpty()) {
                sureSave(entries, offsetK);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void sureSave(Collection<Entry<MemorySegment>> entries, long offsetK) {
        OpenOption[] options = {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

        try (Arena arenaWrite = Arena.ofConfined();
             FileChannel fileChannel = FileChannel.open(filePath.resolve(String.valueOf(fileName)), options)) {

            MemorySegment fileSegment = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, offsetV, arenaWrite);

            fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, offsetK);
            log2Save(0, entries.size() - 1, fileSegment, entries.iterator(), offsetK);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long log2Save(int lo, int hi, MemorySegment fileSegment, Iterator<Entry<MemorySegment>> iterator,
                          long startOffsetKey) {
        if (hi < lo) {
            return -1;
        }

        int mid = (lo + hi) >>> 1;

        long offsetL = 0L;
        long offsetR = 0L;

        if (lo < mid) {
            offsetL = log2Save(lo, mid - 1, fileSegment, iterator, startOffsetKey);
        }

        Entry<MemorySegment> entry = iterator.next();
        MemorySegment key = entry.key();
        MemorySegment value = entry.value();

        if (mid < hi) {
            offsetR = log2Save(mid + 1, hi, fileSegment, iterator, offsetL != 0 ? offsetL : startOffsetKey);
        }

        if (value != null) {
            offsetV -= value.byteSize();
            MemorySegment.copy(value, 0, fileSegment, offsetV, value.byteSize());
        }

        byte offsetToR = (byte) (offsetR == 0 ? 0 : 1);

        long offsetKey;
        if (offsetR != 0) {
            offsetKey = offsetR;
        } else {
            if (offsetL != 0) {
                offsetKey = offsetL;
            } else {
                offsetKey = startOffsetKey;
            }
        }

        offsetKey -= Byte.BYTES;
        fileSegment.set(ValueLayout.JAVA_BYTE, offsetKey, offsetToR);

        offsetKey -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKey, offsetL);

        offsetKey -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKey, value == null ? 0 : offsetV);

        offsetKey -= key.byteSize();
        MemorySegment.copy(key, 0, fileSegment, offsetKey, key.byteSize());

        offsetKey -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKey, key.byteSize());

        return offsetKey;
    }

}
