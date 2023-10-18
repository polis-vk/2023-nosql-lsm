package ru.vk.itmo.bazhenovkirill;

import ru.vk.itmo.BaseEntry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class SStable {
    private static final String DATA_FILE = "data.db";
    private static final String INDEX_FILE = "index.db";
    private final MemorySegmentComparator cmp = MemorySegmentComparator.getInstance();
    private MemorySegment dataSegment;
    private MemorySegment indexSegment;
    private final Arena arena;

    private int numberOfKeys;

    public SStable(Path directoryPath, Arena arena) throws IOException {
        this.arena = arena;
        if (!Files.exists(directoryPath)) {
            return;
        }

        try (FileChannel dataChannel = FileChannel.open(directoryPath.resolve(DATA_FILE), StandardOpenOption.READ);
             FileChannel indexChannel = FileChannel.open(directoryPath.resolve(INDEX_FILE), StandardOpenOption.READ)) {
            dataSegment = mapSegmentForReading(dataChannel);
            indexSegment = mapSegmentForReading(indexChannel);
        }
        numberOfKeys = indexSegment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
    }

    private MemorySegment mapSegmentForReading(FileChannel channel) throws IOException {
        return channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena).asReadOnly();
    }

    public EntryWithTimestamp<MemorySegment> getData(MemorySegment key) {
        long offset = leftBinarySearch(key);
        long keySize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        if (keySize == key.byteSize()) {
            offset += Long.BYTES;
            MemorySegment possibleKey = dataSegment.asSlice(offset, keySize);
            if (cmp.compare(key, possibleKey) == 0) {
                offset += keySize;
                long valueSize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                MemorySegment value = null;
                if (valueSize != -1) {
                    value = dataSegment.asSlice(offset, valueSize);
                    offset += valueSize;
                }


                long timestamp = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                return new EntryWithTimestamp<>(new BaseEntry<>(key, value), timestamp);
            }
        }
        return null;
    }

    public SSTablePeekableIterator iterator(MemorySegment from, MemorySegment to) {
        long offset = leftBinarySearch(from);
        return new SSTablePeekableIterator(dataSegment, offset, to);
    }

    public long leftBinarySearch(MemorySegment key) {
        if (key == null) return 0;
        int l = -1;
        int r = numberOfKeys;
        while (l < r - 1) {
            int mid = l + (r - l) / 2;
            long offset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Integer.BYTES + (long) mid * Long.BYTES);
            long keySize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

            MemorySegment possibleKey = dataSegment.asSlice(offset + Long.BYTES, keySize);
            int cmpResult = cmp.compare(possibleKey, key);
            if (cmpResult < 0) {
                l = mid;
            } else {
                r = mid;
            }
        }
        return indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, Integer.BYTES + (long) r * Long.BYTES);
    }
}
