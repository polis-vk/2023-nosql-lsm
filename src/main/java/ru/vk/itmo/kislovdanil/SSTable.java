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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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

    private static class IndexRecord {
        public long dataOffset;
        public long dataLength;
        public MemorySegment key;

        public IndexRecord(long dataOffset, long dataLength, MemorySegment key) {
            this.dataOffset = dataOffset;
            this.dataLength = dataLength;
            this.key = key;
        }
    }

    private IndexRecord findByKey(MemorySegment key, List<IndexRecord> records) {
        for (IndexRecord curRecord : records) {
            if (memSegComp.compare(key, curRecord.key) == 0) {
                return curRecord;
            }
        }
        return null;
    }

    private MemorySegment readFileData(RandomAccessFile file, long offset, long len) throws IOException {
        byte[] data = new byte[(int) len];
        file.seek(offset);
        file.readFully(data);
        return MemorySegment.ofArray(data);
    }

    public MemorySegment find(MemorySegment key) throws IOException {
        ByteBuffer summaryBytes = ByteBuffer.wrap(Files.readAllBytes(summaryFile));
        List<IndexRecord> indexRecords = new ArrayList<>();
        Arena arena = Arena.ofAuto();
        try (RandomAccessFile indexRAFile = new RandomAccessFile(indexFile.toString(), "r");
             RandomAccessFile dataRAFile = new RandomAccessFile(dataFile.toString(), "r")) {
            MemorySegment indexMappedFile = indexRAFile.getChannel().map(FileChannel.MapMode.READ_ONLY,
                    0, indexRAFile.length(), arena);
            while (summaryBytes.remaining() > 0) {
                long offset = summaryBytes.getLong();
                long len = summaryBytes.getLong();
                MemorySegment segment = indexMappedFile.asSlice(offset, len + 2 * Long.BYTES);
                ByteBuffer range = segment.asSlice(len, 2 * Long.BYTES).asByteBuffer();
                long dataOffset = range.getLong();
                long dataLen = range.getLong();
                indexRecords.add(new IndexRecord(dataOffset, dataLen, segment.asSlice(0, len)));
            }
            IndexRecord result = findByKey(key, indexRecords);
            if (result == null) return null;
            return readFileData(dataRAFile, result.dataOffset, result.dataLength);
        }
    }
}
