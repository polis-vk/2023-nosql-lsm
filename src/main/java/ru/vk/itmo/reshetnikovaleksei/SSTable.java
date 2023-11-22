package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.iterators.SSTableIterator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SSTable {
    public static final String DATA_PREFIX = "data-";
    public static final String DATA_TMP = "data.tmp";
    public static final String INDEX_PREFIX = "index-";
    public static final String INDEX_TMP = "index.tmp";

    private final MemorySegment dataSegment;
    private final MemorySegment indexSegment;
    private final Path dataPath;
    private final Path indexPath;

    public SSTable(Path basePath, Arena arena, long idx) throws IOException {
        this.dataPath = basePath.resolve(DATA_PREFIX + idx);
        this.indexPath = basePath.resolve(INDEX_PREFIX + idx);

        try (FileChannel dataChannel = FileChannel.open(dataPath, StandardOpenOption.READ)) {
            this.dataSegment = dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataChannel.size(), arena);
        }
        try (FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            this.indexSegment = indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexChannel.size(), arena);
        }
    }

    public Iterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) {
        long indexFrom;

        if (from == null) {
            indexFrom = 0;
        } else {
            indexFrom = getIndexOffsetByKey(from);
        }

        return new SSTableIterator(indexFrom, to, dataSegment, indexSegment);
    }

    public void deleteFiles() throws IOException {
        Files.deleteIfExists(dataPath);
        Files.deleteIfExists(indexPath);
    }

    private long getIndexOffsetByKey(MemorySegment key) {
        long l = 0;
        long r = indexSegment.byteSize() / Long.BYTES - 1;

        while (l <= r) {
            long m = l + (r - l) / 2;

            long dataOffset = indexSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Long.BYTES);
            long currKeySize = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
            dataOffset += Long.BYTES;

            long comparing = MemorySegmentComparator.getInstance().compare(
                    key, dataSegment, dataOffset, dataOffset + currKeySize);
            if (comparing > 0) {
                l = m + 1;
            } else if (comparing < 0) {
                r = m - 1;
            } else {
                return m * Long.BYTES;
            }

        }

        return l * Long.BYTES;
    }
}
