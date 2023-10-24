package ru.vk.itmo.viktorkorotkikh;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SSTable implements Closeable {

    private final Arena arena;

    private final MemorySegment mappedSSTableFile;

    private static final String FILE_NAME = "sstable";

    private static final String FILE_EXTENSION = ".db";

    private static final String TMP_FILE_EXTENSION = ".tmp";

    private static final long METADATA_SIZE = Long.BYTES;

    private static final long ENTRY_METADATA_SIZE = Long.BYTES;
    private final int index;

    private SSTable(Arena arena, MemorySegment mappedSSTableFile, int index) {
        this.arena = arena;
        this.mappedSSTableFile = mappedSSTableFile;
        this.index = index;
    }

    private static Comparator<Path> ssTablePathComparator() {
        return (p1, p2) -> {
            String p1String = p1.getFileName().toString();
            int p1Index = Integer.parseInt(
                    p1String.substring(FILE_NAME.length(), p1String.length() - FILE_EXTENSION.length())
            );
            String p2String = p2.getFileName().toString();
            int p2Index = Integer.parseInt(
                    p2String.substring(FILE_NAME.length(), p2String.length() - FILE_EXTENSION.length())
            );
            return Integer.compare(p1Index, p2Index);
        };
    }

    public static List<SSTable> load(Path basePath) throws IOException {
        List<Path> ssTablePaths;
        try (Stream<Path> paths = Files.walk(basePath, 1)) {
            ssTablePaths = paths.filter(Files::isRegularFile)
                    .filter(filePath -> filePath.getFileName().toString().endsWith(FILE_EXTENSION))
                    .sorted(ssTablePathComparator())
                    .collect(Collectors.toList());
        } catch (NoSuchFileException e) {
            return new ArrayList<>();
        }
        List<SSTable> ssTables = new ArrayList<>(ssTablePaths.size());
        for (int i = 0; i < ssTablePaths.size(); i++) {
            Path ssTablePath = ssTablePaths.get(i);
            Arena arena;
            MemorySegment mappedSSTableFile;
            try (FileChannel fileChannel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
                arena = Arena.ofShared();
                mappedSSTableFile = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0L, fileChannel.size(), arena);
                ssTables.add(new SSTable(arena, mappedSSTableFile, i));
            } catch (IOException e) {
                ssTables.forEach(SSTable::close);
                throw new SSTableReadException(e);
            }
        }
        return ssTables;
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

    public static void save(Collection<Entry<MemorySegment>> entries, int fileIndex, Path basePath) throws IOException {
        if (entries.isEmpty()) return;
        Path tmpSSTable = basePath.resolve(FILE_NAME + fileIndex + FILE_EXTENSION + TMP_FILE_EXTENSION);

        Files.deleteIfExists(tmpSSTable);
        Files.createFile(tmpSSTable);

        MemorySegment mappedSSTableFile;

        long entriesDataSize = 0;

        for (Entry<MemorySegment> entry : entries) {
            entriesDataSize += getEntrySize(entry);
        }

        long entriesDataOffset = METADATA_SIZE + ENTRY_METADATA_SIZE * entries.size();

        try (Arena arena = Arena.ofConfined();
             FileChannel channel = FileChannel.open(
                     tmpSSTable, StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING
             )
        ) {
            mappedSSTableFile = channel.map(
                    FileChannel.MapMode.READ_WRITE,
                    0L,
                    entriesDataOffset + entriesDataSize,
                    arena
            );

            mappedSSTableFile.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entries.size());

            long index = 0;
            long offset = entriesDataOffset;
            for (Entry<MemorySegment> entry : entries) {
                mappedSSTableFile.set(
                        ValueLayout.JAVA_LONG_UNALIGNED,
                        METADATA_SIZE + index * ENTRY_METADATA_SIZE,
                        offset
                );
                offset += getEntrySize(entry);
                index++;
            }

            offset = entriesDataOffset;
            for (Entry<MemorySegment> entry : entries) {
                offset += writeMemorySegment(mappedSSTableFile, entry.key(), offset);
                offset += writeMemorySegment(mappedSSTableFile, entry.value(), offset);
            }

            mappedSSTableFile.force();
            Files.move(
                    tmpSSTable,
                    basePath.resolve(FILE_NAME + fileIndex + FILE_EXTENSION),
                    StandardCopyOption.ATOMIC_MOVE
            );
        }
    }

    private static long getEntrySize(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return Long.BYTES + entry.key().byteSize() + Long.BYTES;
        }
        return Long.BYTES + entry.key().byteSize() + Long.BYTES + entry.value().byteSize();
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
        long entriesSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        return mappedSSTableFile.get(
                ValueLayout.JAVA_LONG_UNALIGNED,
                METADATA_SIZE + (entriesSize - 1) * ENTRY_METADATA_SIZE
        );
    }

    private long getEntryOffset(MemorySegment key, SearchOption searchOption) {
        // binary search
        long entriesSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
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
                    case LT -> keySizeOffset - Long.BYTES;
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

    @Override
    public void close() {
        arena.close();
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
        int getPriority() {
            return index;
        }

        @Override
        public MemorySegment getPointerSrc() {
            return mappedSSTableFile;
        }

        @Override
        public long getPointerSrcOffset() {
            return fromPosition + Long.BYTES;
        }

        @Override
        boolean isPointerOnTombstone() {
            long keySize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, fromPosition);
            long valueOffset = fromPosition + Long.BYTES + keySize;
            long valueSize = mappedSSTableFile.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);
            return valueSize == -1;
        }

        @Override
        public long getPointerSrcSize() {
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
            fromPosition += getEntrySize(entry);
            return entry;
        }
    }
}
