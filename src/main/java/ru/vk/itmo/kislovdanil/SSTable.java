package ru.vk.itmo.kislovdanil;

import ru.vk.itmo.Entry;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.NavigableMap;

public class SSTable {
    private final Path summaryFile;
    private final Path indexFile;
    private final Path dataFile;
    private final Comparator<MemorySegment> memSegComp;

    public static final class ByteUtils {
        private static final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

        private ByteUtils() {

        }

        public static byte[] longToBytes(long data) {
            buffer.putLong(data);
            byte[] result = buffer.array();
            buffer.rewind();
            return result;
        }
    }

    private void createIfNotExists(Path path) throws IOException {
        if (Files.notExists(path)) {
            Files.createDirectories(path.getParent());
            Files.createFile(path);
        }
    }

    private void recreate(Path path) throws IOException {
        Files.delete(path);
        Files.createFile(path);
    }

    public SSTable(Path basePath, Comparator<MemorySegment> memSegComp) throws IOException {
        summaryFile = basePath.resolve("summary");
        this.memSegComp = memSegComp;
        createIfNotExists(summaryFile);
        indexFile = basePath.resolve("index");
        createIfNotExists(indexFile);
        dataFile = basePath.resolve("data");
        createIfNotExists(dataFile);
    }

    private void removeOldData() throws IOException {
        recreate(summaryFile);
        recreate(indexFile);
        recreate(dataFile);
    }

    public void write(NavigableMap<MemorySegment, Entry<MemorySegment>> memTable) throws IOException {
        removeOldData();
        long currentDataOffset = 0;
        long currentIndexOffset = 0;
        try (FileOutputStream summaryOutput = new FileOutputStream(summaryFile.toString());
             FileOutputStream indexOutput = new FileOutputStream(indexFile.toString());
             FileOutputStream dataOutput = new FileOutputStream(dataFile.toString())) {
            for (Entry<MemorySegment> entry : memTable.values()) {
                MemorySegment value = entry.value();
                MemorySegment key = entry.key();
                dataOutput.write(value.toArray(ValueLayout.JAVA_BYTE));
                indexOutput.write(key.toArray(ValueLayout.JAVA_BYTE));
                indexOutput.write(ByteUtils.longToBytes(currentDataOffset));
                indexOutput.write(ByteUtils.longToBytes(value.byteSize()));
                summaryOutput.write(ByteUtils.longToBytes(currentIndexOffset));
                summaryOutput.write(ByteUtils.longToBytes(key.byteSize()));
                currentDataOffset += value.byteSize();
                currentIndexOffset += key.byteSize() + 2 * Long.BYTES;
            }
        }
    }

    private static class Range {
        public long offset;
        public long length;

        public Range(long offset, long length) {
            this.offset = offset;
            this.length = length;
        }
    }

    private MemorySegment readFileData(RandomAccessFile file, long offset, long len) throws IOException {
        byte[] data = new byte[(int) len];
        file.seek(offset);
        file.readFully(data);
        return MemorySegment.ofArray(data);
    }

    private Range readRange(MemorySegment segment, long offset) {
        ByteBuffer buffer = segment.asSlice(offset, 2 * Long.BYTES).asByteBuffer();
        return new Range(buffer.getLong(), buffer.getLong());
    }

    private Range findByKey(MemorySegment key, MemorySegment indexFile, MemorySegment summaryFile) {
        long left = 0;
        long right = (summaryFile.byteSize() / (2 * Long.BYTES)) - 1;
        while (right - left > 2) {
            long middle = (right + left) / 2;
            Range indexRange = readRange(summaryFile, middle * Long.BYTES * 2);
            MemorySegment curKey = indexFile.asSlice(indexRange.offset, indexRange.length);
            int compRes = memSegComp.compare(key, curKey);
            if (compRes == 0) readRange(indexFile, indexRange.offset + indexRange.length);
            if (compRes < 0) {
                right = middle;
            } else {
                left = middle;
            }
        }
        for (long i = left; i <= right; i++) {
            Range indexRange = readRange(summaryFile, i * Long.BYTES * 2);
            MemorySegment curKey = indexFile.asSlice(indexRange.offset, indexRange.length);
            if (memSegComp.compare(key, curKey) == 0) {
                return readRange(indexFile, indexRange.offset + indexRange.length);
            }
        }
        return null;
    }

    public MemorySegment find(MemorySegment key) throws IOException {
        Arena arena = Arena.ofAuto();
        MemorySegment indexMappedFile;
        MemorySegment summaryMappedFile;
        try (RandomAccessFile indexRAFile = new RandomAccessFile(indexFile.toString(), "r");
             RandomAccessFile summaryRAFile = new RandomAccessFile(summaryFile.toString(), "r")) {
            indexMappedFile = indexRAFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
                    0, indexRAFile.length(), arena);
            summaryMappedFile = summaryRAFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
                    0, summaryRAFile.length(), arena);
        }
        Range result = findByKey(key, indexMappedFile, summaryMappedFile);
        if (result == null) return null;
        try (RandomAccessFile dataRAFile = new RandomAccessFile(dataFile.toString(), "r")) {
            return readFileData(dataRAFile, result.offset, result.length);
        }
    }
}
