package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class SSTable {

    private final MemorySegment mappedSSTableFile;

    private static final String FILE_NAME = "sstable";

    private static final String INDEX_FILE_NAME = "index.idx";

    private static final String FILE_EXTENSION = ".db";

    private static final String SSTABLE_INDEX_EXTENSION = ".index";

    private static final String COMPRESSION_INFO_EXTENSION = ".compressionInfo";

    private static final String TMP_FILE_EXTENSION = ".tmp";

    private static final long METADATA_SIZE = Long.BYTES + 1L;

    private static final long ENTRY_METADATA_SIZE = Long.BYTES;
    private final int index;

    private final boolean hasNoTombstones;

    public static Path compressionInfoName(
            final Path baseDir,
            final int fileIndex
    ) {
        return baseDir.resolve(FILE_NAME + fileIndex + COMPRESSION_INFO_EXTENSION);
    }

    public static Path indexName(
            final Path baseDir,
            final int fileIndex
    ) {
        return baseDir.resolve(fileIndex + SSTABLE_INDEX_EXTENSION);
    }

    public static Path dataName(
            final Path baseDir,
            final int fileIndex
    ) {
        return baseDir.resolve(FILE_NAME + fileIndex + FILE_EXTENSION);
    }

    public static Path tempCompressionInfoName(
            final Path baseDir,
            final int fileIndex
    ) {
        return baseDir.resolve(FILE_NAME + fileIndex + COMPRESSION_INFO_EXTENSION + TMP_FILE_EXTENSION);
    }

    public static Path tempIndexName(
            final Path baseDir,
            final int fileIndex
    ) {
        return baseDir.resolve(FILE_NAME + fileIndex + SSTABLE_INDEX_EXTENSION + TMP_FILE_EXTENSION);
    }

    public static Path tempDataName(
            final Path baseDir,
            final int fileIndex
    ) {
        return baseDir.resolve(FILE_NAME + fileIndex + FILE_EXTENSION + TMP_FILE_EXTENSION);
    }

    private SSTable(MemorySegment mappedSSTableFile, int index, boolean hasNoTombstones) {
        this.mappedSSTableFile = mappedSSTableFile;
        this.index = index;
        this.hasNoTombstones = hasNoTombstones;
    }

    public static List<SSTable> load(Arena arena, Path basePath) throws IOException {
        if (checkIfCompactedExists(basePath)) {
            finalizeCompaction(basePath);
        }

        Path indexTmp = basePath.resolve(INDEX_FILE_NAME + TMP_FILE_EXTENSION);
        Path indexFile = basePath.resolve(INDEX_FILE_NAME);

        if (!Files.exists(indexFile)) {
            if (Files.exists(indexTmp)) {
                Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } else {
                if (!Files.exists(basePath)) {
                    Files.createDirectory(basePath);
                }
                Files.createFile(indexFile);
            }
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        List<SSTable> ssTables = new ArrayList<>(existedFiles.size());
        for (int i = 0; i < existedFiles.size(); i++) {
            Path ssTablePath = basePath.resolve(existedFiles.get(i));
            ssTables.add(loadOne(arena, ssTablePath, i));
        }

        return ssTables;
    }

    public static SSTable loadOne(Arena arena, Path ssTablePath, int index) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            MemorySegment mappedSSTableFile =
                    fileChannel.map(FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size(), arena);
            if (mappedSSTableFile.byteSize() == 0) {
                throw new IOException("Couldn't read empty ssTable file");
            }
            boolean hasNoTombstones = mappedSSTableFile.get(ValueLayout.JAVA_BOOLEAN, 0);
            return new SSTable(mappedSSTableFile, index, hasNoTombstones);
        }
    }

    public static boolean isCompacted(List<SSTable> ssTables) {
        return ssTables.isEmpty() || (ssTables.size() == 1 && ssTables.getFirst().hasNoTombstones);
    }

    public SSTableIterator iterator(MemorySegment from, MemorySegment to) {
        long fromPosition = getMinKeySizeOffset();
        long toPosition = getMaxKeySizeOffset();
        if (from != null) {
            fromPosition = getEntryOffset(from, SearchOption.GTE);
            if (fromPosition == -1) {
                return new SSTableIterator(0, -1);
            }
        }
        if (to != null) {
            toPosition = getEntryOffset(to, SearchOption.LT);
            if (toPosition == -1) {
                return new SSTableIterator(0, -1);
            }
        }

        return new SSTableIterator(fromPosition, toPosition);
    }

    public static List<SSTable.SSTableIterator> ssTableIterators(
            List<SSTable> ssTables,
            MemorySegment from,
            MemorySegment to
    ) {
        return ssTables.stream().map(ssTable -> ssTable.iterator(from, to)).toList();
    }

    public static void save(MemTable memTable, int fileIndex, Path basePath) throws IOException {
        if (memTable.isEmpty()) return;

        final Path indexTmp = basePath.resolve(INDEX_FILE_NAME + TMP_FILE_EXTENSION);
        final Path indexFile = basePath.resolve(INDEX_FILE_NAME);

        try {
            Files.createFile(indexFile);
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }

        Path tmpSSTable = basePath.resolve(FILE_NAME + fileIndex + FILE_EXTENSION + TMP_FILE_EXTENSION);

        Files.deleteIfExists(tmpSSTable);
        Files.createFile(tmpSSTable);

        save(memTable.values()::iterator, memTable.values().size(), memTable.getByteSize(), tmpSSTable);
        String newFileName = FILE_NAME + fileIndex + FILE_EXTENSION;
        Files.move(
                tmpSSTable,
                basePath.resolve(newFileName),
                StandardCopyOption.ATOMIC_MOVE
        );

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        List<String> list = new ArrayList<>(existedFiles.size() + 1);
        list.addAll(existedFiles);
        list.add(newFileName);
        Files.write(
                indexTmp,
                list,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.deleteIfExists(indexFile);

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
    }

    private static Path save(
            Supplier<? extends Iterator<Entry<MemorySegment>>> iteratorSupplier,
            int entriesSize,
            long entriesDataSize,
            Path tmpSSTable
    ) throws IOException {
        if (entriesSize == 0) {
            Files.deleteIfExists(tmpSSTable);
            return tmpSSTable;
        }

        long entriesDataOffset = METADATA_SIZE + ENTRY_METADATA_SIZE * entriesSize;

        try (Arena arena = Arena.ofConfined();
             FileChannel channel = FileChannel.open(
                     tmpSSTable, StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING
             )
        ) {
            MemorySegment mappedSSTableFile = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0L,
                    entriesDataOffset + entriesDataSize,
                    arena
            );

            mappedSSTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, 1, entriesSize);

            writeIndex(iteratorSupplier.get(), mappedSSTableFile, entriesDataOffset);

            long offset = entriesDataOffset;
            Iterator<Entry<MemorySegment>> entryIterator = iteratorSupplier.get();
            // by default file contains JAVA_BYTE == 0 with offset 0
            // so if we have possibly compacted file and it has JAVA_BYTE == 0 with offset 0
            // then it is corrupted
            // otherwise we have unbroken file without tombstones (compacted)
            // owing to this we use hasNoTombstones condition
            boolean hasNoTombstones = true;
            while (entryIterator.hasNext()) {
                Entry<MemorySegment> entry = entryIterator.next();
                if (entry.value() == null) {
                    hasNoTombstones = false;
                }
                offset += writeMemorySegment(mappedSSTableFile, entry.key(), offset);
                offset += writeMemorySegment(mappedSSTableFile, entry.value(), offset);
            }

            mappedSSTableFile.force();
            mappedSSTableFile.set(ValueLayout.JAVA_BOOLEAN, 0, hasNoTombstones);
            mappedSSTableFile.force();
            return tmpSSTable;
        }
    }

    private static void writeIndex(
            Iterator<Entry<MemorySegment>> iterator,
            MemorySegment mappedSSTableFile,
            long offset
    ) {
        long index = 0;
        while (iterator.hasNext()) {
            mappedSSTableFile.set(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    METADATA_SIZE + index * ENTRY_METADATA_SIZE,
                    offset
            );
            if (iterator instanceof MergeIterator.MergeIteratorWithTombstoneFilter mergeIterator) {
                offset += mergeIterator.getPointerSizeAndShift();
            } else {
                Entry<MemorySegment> entry = iterator.next();
                offset += Utils.getEntrySize(entry);
            }
            index++;
        }
    }

    public static void compact(
            Supplier<MergeIterator.MergeIteratorWithTombstoneFilter> data,
            Path basePath
    ) throws IOException {
        Path tmpSSTable = basePath.resolve("_compacted_" + FILE_NAME + FILE_EXTENSION + TMP_FILE_EXTENSION);
        Files.deleteIfExists(tmpSSTable);
        Files.createFile(tmpSSTable);
        EntriesMetadata entriesMetadata = data.get().countEntities();
        Path compacted = save(data, entriesMetadata.count(), entriesMetadata.entriesDataSize(), tmpSSTable);
        Files.move(compacted, getCompactedFilePath(basePath), StandardCopyOption.ATOMIC_MOVE);
        finalizeCompaction(basePath);
    }

    private static Path getCompactedFilePath(Path basePath) {
        return basePath.resolve("_compacted_" + FILE_NAME + FILE_EXTENSION);
    }

    private static boolean checkIfCompactedExists(Path basePath) {
        Path compacted = getCompactedFilePath(basePath);
        if (!Files.exists(compacted)) {
            return false;
        }
        try (Arena arena = Arena.ofConfined()) {
            return !loadOne(arena, compacted, 0).hasNoTombstones;
        } catch (IOException ignored) {
            return false;
        }
    }

    private static void finalizeCompaction(Path storagePath) throws IOException {
        try (Stream<Path> stream =
                     Files.find(
                             storagePath,
                             1,
                             (path, ignored) -> {
                                 String fileName = path.getFileName().toString();
                                 return fileName.startsWith(FILE_NAME) && fileName.endsWith(FILE_EXTENSION);
                             })) {
            stream.forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        Path indexTmp = storagePath.resolve(INDEX_FILE_NAME + TMP_FILE_EXTENSION);
        Path indexFile = storagePath.resolve(INDEX_FILE_NAME);

        Files.deleteIfExists(indexFile);
        Files.deleteIfExists(indexTmp);

        Path compactionFile = getCompactedFilePath(storagePath);
        boolean noData = Files.size(compactionFile) == 0;

        Files.write(
                indexTmp,
                noData ? Collections.emptyList() : Collections.singleton(FILE_NAME + "0" + FILE_EXTENSION),
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING
        );

        Files.move(indexTmp, indexFile, StandardCopyOption.ATOMIC_MOVE);
        if (noData) {
            Files.delete(compactionFile);
        } else {
            Files.move(
                    compactionFile,
                    storagePath.resolve(FILE_NAME + "0" + FILE_EXTENSION),
                    StandardCopyOption.ATOMIC_MOVE
            );
        }
    }

    private static long writeMemorySegment(
            MemorySegment ssTableMemorySegment,
            MemorySegment memorySegmentToWrite,
            long offset
    ) {
        if (memorySegmentToWrite == null) {
            ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1);
            return Long.BYTES;
        }
        long memorySegmentToWriteSize = memorySegmentToWrite.byteSize();
        // write memorySegment size and memorySegment
        ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, memorySegmentToWriteSize);
        MemorySegment.copy(
                memorySegmentToWrite,
                0,
                ssTableMemorySegment,
                offset + Long.BYTES,
                memorySegmentToWrite.byteSize()
        );
        return Long.BYTES + memorySegmentToWriteSize;
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        long entryOffset = getEntryOffset(key, SearchOption.EQ);
        if (entryOffset == -1) {
            return null;
        }
        return getByIndex(entryOffset);
    }

    private long getEntriesSize() {
        return mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, 1);
    }

    private Entry<MemorySegment> getByIndex(long index) {
        long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, index);
        MemorySegment savedKey = mappedSSTableFile.asSlice(index + Long.BYTES, keySize);

        long valueOffset = index + Long.BYTES + keySize;
        long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);
        if (valueSize == -1) {
            return new BaseEntry<>(savedKey, null);
        }
        return new BaseEntry<>(savedKey, mappedSSTableFile.asSlice(valueOffset + Long.BYTES, valueSize));
    }

    private long getMinKeySizeOffset() {
        return mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, METADATA_SIZE);
    }

    private long getMaxKeySizeOffset() {
        long entriesSize = getEntriesSize();
        return mappedSSTableFile.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                METADATA_SIZE + (entriesSize - 1) * ENTRY_METADATA_SIZE
        );
    }

    private long getEntryOffset(MemorySegment key, SearchOption searchOption) {
        // binary search
        long entriesSize = getEntriesSize();
        long left = 0;
        long right = entriesSize - 1;
        while (left <= right) {
            long mid = (right + left) / 2;
            long keySizeOffset = mappedSSTableFile.get(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    METADATA_SIZE + mid * ENTRY_METADATA_SIZE
            );
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, keySizeOffset);
            long keyOffset = keySizeOffset + Long.BYTES;
            int keyComparison = MemorySegmentComparator.INSTANCE.compare(
                    mappedSSTableFile, keyOffset,
                    keyOffset + keySize,
                    key
            );
            if (keyComparison < 0) {
                left = mid + 1;
            } else if (keyComparison > 0) {
                right = mid - 1;
            } else {
                return switch (searchOption) {
                    case EQ, GTE -> keySizeOffset;
                    case LT -> keySizeOffset - METADATA_SIZE;
                };
            }
        }

        return switch (searchOption) {
            case EQ -> -1;
            case GTE -> {
                if (left == entriesSize) {
                    yield -1;
                } else {
                    yield mappedSSTableFile.get(
                            ValueLayout.JAVA_LONG_UNALIGNED,
                            METADATA_SIZE + left * ENTRY_METADATA_SIZE
                    );
                }
            }
            case LT -> mappedSSTableFile.get(
                    ValueLayout.JAVA_LONG_UNALIGNED,
                    METADATA_SIZE + right * ENTRY_METADATA_SIZE
            );
        };
    }

    private enum SearchOption {
        EQ, GTE, LT
    }

    public final class SSTableIterator extends LSMPointerIterator {
        private long fromPosition;
        private final long toPosition;

        private SSTableIterator(long fromPosition, long toPosition) {
            this.fromPosition = fromPosition;
            this.toPosition = toPosition;
        }

        @Override
        public int getPriority() {
            return index;
        }

        @Override
        public MemorySegment getPointerKeySrc() {
            return mappedSSTableFile;
        }

        @Override
        public long getPointerKeySrcOffset() {
            return fromPosition + Long.BYTES;
        }

        @Override
        public boolean isPointerOnTombstone() {
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, fromPosition);
            long valueOffset = fromPosition + Long.BYTES + keySize;
            long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);
            return valueSize == -1;
        }

        @Override
        public void shift() {
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, fromPosition);
            long valueOffset = fromPosition + Long.BYTES + keySize;
            long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);
            fromPosition += Long.BYTES + keySize + Long.BYTES;
            if (valueSize != -1) {
                fromPosition += valueSize;
            }
        }

        @Override
        public long getPointerSize() {
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, fromPosition);
            long valueOffset = fromPosition + Long.BYTES + keySize;
            long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);
            if (valueSize == -1) {
                return Long.BYTES + keySize + Long.BYTES;
            }
            return Long.BYTES + keySize + Long.BYTES + valueSize;
        }

        @Override
        public long getPointerKeySrcSize() {
            return mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, fromPosition);
        }

        @Override
        public boolean hasNext() {
            return fromPosition <= toPosition;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<MemorySegment> entry = getByIndex(fromPosition);
            fromPosition += Utils.getEntrySize(entry);
            return entry;
        }
    }
}
