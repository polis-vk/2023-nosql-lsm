package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;

public class SSTableWriter {

    private final String fileName;
    private final Path filePath;
    private long offsetV;

    public SSTableWriter(Path path, int fileCount) {
        filePath = path;
        fileName = "database_" + String.format("%010d", fileCount);
    }

    public void save(Collection<Entry<MemorySegment>> entries) {
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
        StandardOpenOption[] options = {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

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

        if (mid < hi) {
            offsetR = log2Save(mid + 1, hi, fileSegment, iterator, offsetL == 0 ? startOffsetKey : offsetL);
        }

        MemorySegment value = entry.value();

        if (value != null) {
            offsetV -= value.byteSize();
            MemorySegment.copy(value, 0, fileSegment, offsetV, value.byteSize());
        }

        byte offsetToR = (byte) (offsetR == 0 ? 0 : 1);

        long offsetKey;
        if (offsetR == 0) {
            if (offsetL == 0) {
                offsetKey = startOffsetKey;
            } else {
                offsetKey = offsetL;
            }
        } else {
            offsetKey = offsetR;
        }

        offsetKey -= Byte.BYTES;
        fileSegment.set(ValueLayout.JAVA_BYTE, offsetKey, offsetToR);

        offsetKey -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKey, offsetL);

        offsetKey -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKey, value == null ? 0 : offsetV);

        MemorySegment key = entry.key();

        offsetKey -= key.byteSize();
        MemorySegment.copy(key, 0, fileSegment, offsetKey, key.byteSize());

        offsetKey -= Long.BYTES;
        fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetKey, key.byteSize());

        return offsetKey;
    }

}
