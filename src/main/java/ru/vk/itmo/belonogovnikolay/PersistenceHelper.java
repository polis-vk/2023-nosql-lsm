package ru.vk.itmo.belonogovnikolay;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Util class for write, read, and persistence recovery operations when the {@link InMemoryTreeDao DAO} is restarted.
 *
 * @author Belonogov Nikolay
 */
public final class PersistenceHelper {

    private final Path basePath;
    private final MemorySegmentComparator segmentComparator;

    private MemorySegment dataMappedSegment;
    private MemorySegment offsetMappedSegment;
    private long[] positionOffsets;
    private int position;
    private Path pathToDataFile;
    private Path pathToOffsetFile;
    private long offsetFileSize;
    private boolean isReadingPrepared;
    private Arena readingDataArena;
    private Arena readingOffsetArena;

    private PersistenceHelper(Path basePath) {
        this.basePath = basePath;
        this.segmentComparator = new MemorySegmentComparator();
        resolvePaths();
    }

    /**
     * Returns instance of PersistenceHelper class.
     *
     * @param basePath directory for storing snapshots.
     * @return PersistentHelper instance.
     * @throws NullPointerException is thrown when the path to the directory with snapshot files is not specified.
     */
    public static PersistenceHelper newInstance(Path basePath) {
        return new PersistenceHelper(basePath);
    }

    /**
     * The function writes to the file specified in the config. If the config is not specified, an exception is thrown.
     *
     * @param entries data to be written to disk.
     * @throws IOException is thrown when exceptions occur while working with a file.
     */
    public void writeEntries(NavigableMap<MemorySegment, Entry<MemorySegment>> entries) throws IOException {

        int size = entries.size();

        positionOffsets = new long[size * 2 + 1];

        long fileSize = 0;
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : entries.entrySet()) {
            fileSize += entry.getKey().byteSize() + entry.getValue().value().byteSize();
        }

        Files.deleteIfExists(pathToDataFile);
        Files.deleteIfExists(pathToOffsetFile);

        Files.createFile(pathToDataFile);
        Files.createFile(pathToOffsetFile);

        try (Arena dataArena = Arena.ofConfined()) {
            try (Arena offsetArena = Arena.ofConfined()) {

                this.dataMappedSegment = mapFilesWriteRead(pathToDataFile, fileSize, dataArena);
                this.offsetMappedSegment = mapFilesWriteRead(pathToOffsetFile,
                        (long) Long.BYTES * positionOffsets.length, offsetArena);

                entries.values().forEach(entry -> {
                    long keySize = entry.key().byteSize();
                    long valueSize = entry.value().byteSize();
                    positionOffsets[position + 1] = positionOffsets[position] + keySize;
                    positionOffsets[position + 2] = positionOffsets[position + 1] + valueSize;
                    this.dataMappedSegment.asSlice(positionOffsets[position], keySize).copyFrom(entry.key());
                    this.dataMappedSegment.asSlice(positionOffsets[position + 1], valueSize).copyFrom(entry.value());
                    position = position + 2;
                });

                offsetMappedSegment
                        .asSlice(0L, (long) Long.BYTES * positionOffsets.length)
                        .copyFrom(MemorySegment.ofArray(positionOffsets));
            }
        } finally {
            if (this.readingDataArena != null) {
                this.readingDataArena.close();
                this.readingDataArena = null;
            }

            if (this.readingOffsetArena != null) {
                this.readingOffsetArena.close();
                this.readingOffsetArena = null;
            }
        }
    }

    /**
     * Returns entry of data which is read from file.
     *
     * @param key is search key of data entry which is read from file.
     * @return entry of data.
     * @throws IOException is thrown when exceptions occur while working with a file.
     */
    public Entry<MemorySegment> readEntry(MemorySegment key) throws IOException {

        if (Files.notExists(pathToDataFile) || Files.notExists(pathToOffsetFile)) {
            return null;
        }

        if (!isReadingPrepared) {
            readingPreparation();
            this.isReadingPrepared = true;
        }

        long index = 0;
        long beginLong;
        long endLong;
        long keyValueSize;
        long offsetFileOffsetCount = (this.offsetFileSize - Long.BYTES) / 8 - 1;

        while (index < offsetFileOffsetCount) {
            beginLong = offsetMappedSegment.getAtIndex(ValueLayout.JAVA_LONG, index);
            endLong = offsetMappedSegment.getAtIndex(ValueLayout.JAVA_LONG, index + 1);
            keyValueSize = endLong - beginLong;
            MemorySegment keySegment = dataMappedSegment.asSlice(beginLong, keyValueSize);
            if (this.segmentComparator.compare(keySegment, key) == 0) {
                keyValueSize = offsetMappedSegment.getAtIndex(ValueLayout.JAVA_LONG, index + 2) - endLong;
                return new BaseEntry<>(keySegment, dataMappedSegment.asSlice(endLong, keyValueSize));
            }
            index++;
        }
        return null;
    }

    /**
     * Function of mapping MemorySegment and data file in READ-ONLY mode.
     *
     * @param filePath file path.
     * @param byteSize file size (offset).
     * @return {@link MemorySegment} which map with file.
     * @throws IOException is thrown when exceptions occur while working with a file.
     */
    private MemorySegment mapDataFileReadOnly(Path filePath, long byteSize) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            this.readingDataArena = Arena.ofConfined();
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, byteSize, this.readingDataArena);
        }
    }

    /**
     * Function of mapping MemorySegment and offset file in READ-ONLY mode.
     *
     * @param filePath file path.
     * @param byteSize file size (offset).
     * @return {@link MemorySegment} which map with file.
     * @throws IOException is thrown when exceptions occur while working with a file.
     */
    private MemorySegment mapOffsetFileReadOnly(Path filePath, long byteSize) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ)) {
            this.readingOffsetArena = Arena.ofConfined();
            return channel.map(FileChannel.MapMode.READ_ONLY, 0, byteSize, this.readingOffsetArena);
        }
    }

    /**
     * Function of mapping MemorySegment and file in READ-WRITE mode.
     *
     * @param filePath file path.
     * @param byteSize file size (offset).
     * @return {@link MemorySegment} which map with file.
     * @throws IOException is thrown when exceptions occur while working with a file.
     */
    private MemorySegment mapFilesWriteRead(Path filePath, long byteSize, Arena arena) throws IOException {
        try (FileChannel channel = FileChannel.open(filePath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            return channel.map(FileChannel.MapMode.READ_WRITE, 0, byteSize, arena);
        }
    }

    /**
     * Function resolves {@link #basePath} to file {@link #pathToDataFile}, {@link #pathToOffsetFile} paths.
     */
    private void resolvePaths() {
        this.pathToDataFile = this.basePath.resolve(FileType.DATA.toString());
        this.pathToOffsetFile = this.basePath.resolve(FileType.OFFSET.toString());
    }

    /**
     * Until function to prepare for reading operations.
     *
     * @throws IOException is thrown when exceptions occur while working with a file.
     */
    private void readingPreparation() throws IOException {
        this.offsetFileSize = Files.size(pathToOffsetFile);
        long dataFileSize = Files.size(pathToDataFile);

        this.dataMappedSegment = mapDataFileReadOnly(pathToDataFile, dataFileSize);
        this.offsetMappedSegment = mapOffsetFileReadOnly(pathToOffsetFile, this.offsetFileSize);
    }
}
