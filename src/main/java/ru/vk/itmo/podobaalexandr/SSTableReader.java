package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Stream;

public class SSTableReader implements Closeable {

    private final List<MemorySegment> pages = new ArrayList<>();
    private Arena arena;

    public SSTableReader(Path filePath) {
        boolean created = false;

        if (Files.exists(filePath)) {
            arena = Arena.ofShared();
            try (Stream<Path> stream = Files.list(filePath).sorted()) {
                List<Path> files = stream.toList();
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

    }

    public int size() {
        return pages.size();
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

    public Entry<MemorySegment> get(MemorySegment keySearch) {

        Entry<MemorySegment> res = null;

        for (MemorySegment page : pages) {
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

    public Collection<Entry<MemorySegment>> allPagesFromTo(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map,
                                MemorySegment from, MemorySegment to) {
        if (size() == 0 || compareSegments(from, from.byteSize(), to, 0, to.byteSize()) >= 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (MemorySegment page : pages) {
            long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            allPageFromTo(page, entries, Long.BYTES, keysSize, from, to);
        }

        return entries.values();
    }

    public Collection<Entry<MemorySegment>> allPagesTo(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map, MemorySegment to) {
        if (size() == 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (MemorySegment page: pages) {
            long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            allPageTo(page, entries, Long.BYTES, keysSize, to);
        }

        return entries.values();
    }

    public Collection<Entry<MemorySegment>> allPagesFrom(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map, MemorySegment from) {
        if (size() == 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (MemorySegment page: pages) {
            long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            allPageFrom(page, entries, Long.BYTES, keysSize, from);
        }

        return entries.values();
    }

    public Collection<Entry<MemorySegment>> allPages(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> map) {
        if (size() == 0) {
            return map.values();
        }

        NavigableMap<MemorySegment, Entry<MemorySegment>> entries = new TreeMap<>(map);
        for (MemorySegment page: pages) {
            long keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
            allPage(page, entries, Long.BYTES, keysSize);
        }

        return entries.values();
    }

    //Invariant: FROM < TO
    private void allPageFromTo(MemorySegment page, NavigableMap<MemorySegment, Entry<MemorySegment>> entries,
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

    private void allPageTo(MemorySegment page, NavigableMap<MemorySegment, Entry<MemorySegment>> entries,
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

    private void allPageFrom(MemorySegment page, NavigableMap<MemorySegment, Entry<MemorySegment>> entries,
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

    private void allPage(MemorySegment page, NavigableMap<MemorySegment, Entry<MemorySegment>> entries,
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

    @Override
    public void close() {
        arena.close();
    }

    public boolean isAlive() {
        return arena.scope().isAlive();
    }

    public boolean isArenaPresented() {
        return arena != null;
    }
}