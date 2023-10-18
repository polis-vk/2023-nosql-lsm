package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;

public class SSTable {

    private final long keysSize;

    private final MemorySegment page;

    public SSTable(Path file, Arena arena) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            long size = Files.size(file);
            page = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
        }

        keysSize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    private boolean isOutOfKeyOffset(long offset) {
        return keysSize <= offset || offset == 0;
    }

    //Invariant: FROM < TO
    public void allPageFromTo(NavigableMap<MemorySegment, Entry<MemorySegment>> entries,
                               long offset, MemorySegment from, MemorySegment to) {
        if (isOutOfKeyOffset(offset)) {
            return;
        }

        Entry<MemorySegment> last;
        long offsetLocal = offset;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
        offsetLocal += Long.BYTES;

        long compareFrom = MemorySegmentUtils.compareSegments(from, from.byteSize(), page, offsetLocal, offsetLocal + keySize);
        long compareTo = MemorySegmentUtils.compareSegments(to, to.byteSize(), page, offsetLocal, offsetLocal + keySize);

        if (compareFrom < 0 && compareTo > 0) {
            last = MemorySegmentUtils.getKeyValueFromOffset(page, offsetLocal, keySize, keysSize);
            offsetLocal += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            allPageFrom(entries, offsetToL, from);
            offsetLocal += Long.BYTES;

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offsetLocal);
            long rightOffset = isRightHere == 1 ? offsetLocal + Byte.BYTES : 0;
            allPageTo(entries, rightOffset, to);
        } else if (compareFrom == 0) {
            last = MemorySegmentUtils.getKeyValueFromOffset(page, offsetLocal, keySize, keysSize);
            offsetLocal += keySize + 2 * Long.BYTES;

            entries.putIfAbsent(last.key(), last);
            long rightOffset = page.get(ValueLayout.JAVA_BYTE, offsetLocal) == 1 ? offsetLocal + Byte.BYTES : 0;
            allPageTo(entries, rightOffset, to);
        } else if (compareTo == 0) {
            offsetLocal += keySize + Long.BYTES;
            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            allPageFrom(entries, offsetToL, from);
        } else if (compareFrom > 0) {
            offsetLocal += keySize + 2 * Long.BYTES;
            long rightOffset = page.get(ValueLayout.JAVA_BYTE, offsetLocal) == 1 ? offsetLocal + Byte.BYTES : 0;
            allPageFromTo(entries, rightOffset, from, to);
        } else {
            offsetLocal += keySize + Long.BYTES;
            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            allPageFromTo(entries, offsetToL, from, to);
        }
    }

    public void allPageTo(NavigableMap<MemorySegment, Entry<MemorySegment>> entries,
                           long offset, MemorySegment to) {
        if (isOutOfKeyOffset(offset)) {
            return;
        }

        Entry<MemorySegment> last;
        long offsetLocal = offset;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
        offsetLocal += Long.BYTES;

        int compare = MemorySegmentUtils.compareSegments(to, to.byteSize(), page, offsetLocal, offsetLocal + keySize);

        if (compare == 0) {
            offsetLocal += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            allPage(entries, offsetToL);
        } else if (compare > 0) {
            last = MemorySegmentUtils.getKeyValueFromOffset(page, offsetLocal, keySize, keysSize);
            offsetLocal += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            offsetLocal += Long.BYTES;

            allPage(entries, offsetToL);

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offsetLocal);
            allPageTo(entries, isRightHere == 1 ? offsetLocal + Byte.BYTES : 0, to);
        } else {
            offsetLocal += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            allPageTo(entries, offsetToL, to);
        }
    }

    public void allPageFrom(NavigableMap<MemorySegment, Entry<MemorySegment>> entries,
                            long offset, MemorySegment from) {
        if (isOutOfKeyOffset(offset)) {
            return;
        }

        Entry<MemorySegment> last;
        long offsetLocal = offset;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
        offsetLocal += Long.BYTES;

        int compare = MemorySegmentUtils.compareSegments(from, from.byteSize(), page, offsetLocal, offsetLocal + keySize);

        if (compare == 0) {
            last = MemorySegmentUtils.getKeyValueFromOffset(page, offsetLocal, keySize, keysSize);
            offsetLocal += 2 * Long.BYTES + keySize;

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offsetLocal);
            allPage(entries, isRightHere == 1 ? offsetLocal + Byte.BYTES : 0);
        } else if (compare > 0) {
            offsetLocal += keySize + 2 * Long.BYTES;
            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offsetLocal);
            allPageFrom(entries, isRightHere == 1 ? offsetLocal + Byte.BYTES : 0, from);
        } else {
            last = MemorySegmentUtils.getKeyValueFromOffset(page, offsetLocal, keySize, keysSize);
            offsetLocal += keySize + Long.BYTES;

            long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
            allPageFrom(entries, offsetToL, from);
            offsetLocal += Long.BYTES;

            entries.putIfAbsent(last.key(), last);

            byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offsetLocal);
            allPage(entries, isRightHere == 1 ? offsetLocal + Byte.BYTES : 0);
        }
    }

    public void allPage(NavigableMap<MemorySegment, Entry<MemorySegment>> entries, long offset) {
        if (isOutOfKeyOffset(offset)) {
            return;
        }

        Entry<MemorySegment> last;
        long offsetLocal = offset;

        long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
        offsetLocal += Long.BYTES;

        last = MemorySegmentUtils.getKeyValueFromOffset(page, offsetLocal, keySize, keysSize);

        offsetLocal += keySize + Long.BYTES;
        long offsetToL = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offsetLocal);
        allPage(entries, offsetToL);

        entries.putIfAbsent(last.key(), last);

        offsetLocal += Long.BYTES;
        byte isRightHere = page.get(ValueLayout.JAVA_BYTE, offsetLocal);
        allPage(entries, isRightHere == 1 ? offsetLocal + Byte.BYTES : 0);
    }

    public Entry<MemorySegment> getFromPage(MemorySegment keySearch) {
        long offset = Long.BYTES;
        Entry<MemorySegment> res = null;

        while (offset < keysSize) {
            long keySize = page.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;

            int compare = MemorySegmentUtils.compareSegments(keySearch, keySearch.byteSize(),
                    page, offset, offset + keySize);

            if (compare == 0) {
                res = MemorySegmentUtils.getKeyValueFromOffset(page, offset, keySize, keysSize);
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

}
