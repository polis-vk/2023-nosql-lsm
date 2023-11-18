package ru.vk.itmo.smirnovdmitrii.outofmemory;

import ru.vk.itmo.Entry;
import ru.vk.itmo.smirnovdmitrii.outofmemory.sstable.SSTable;
import ru.vk.itmo.smirnovdmitrii.outofmemory.sstable.SSTableGroup;
import ru.vk.itmo.smirnovdmitrii.outofmemory.sstable.SSTableIterator;
import ru.vk.itmo.smirnovdmitrii.outofmemory.sstable.SSTableStorage;
import ru.vk.itmo.smirnovdmitrii.outofmemory.sstable.SSTableStorageImpl;
import ru.vk.itmo.smirnovdmitrii.outofmemory.sstable.SSTableUtil;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;
import ru.vk.itmo.smirnovdmitrii.util.iterators.MergeIterator;
import ru.vk.itmo.smirnovdmitrii.util.iterators.WrappedIterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FileDao implements OutMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final String INDEX_FILE_NAME = "index.idx";
    private final MemorySegmentComparator comparator = new MemorySegmentComparator();
    private final SSTableStorage storage;

    private final ExecutorService compactor = Executors.newSingleThreadExecutor();
    private final Path basePath;

    public FileDao(final Path basePath) {
        this.basePath = basePath;
        try {
            Files.createDirectories(basePath);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final Path indexFilePath = basePath.resolve(INDEX_FILE_NAME);
        try {
            if (!Files.exists(indexFilePath)) {
                Files.createFile(indexFilePath);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        try {
            this.storage = new SSTableStorageImpl(basePath, INDEX_FILE_NAME);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        for (final SSTable ssTable : storage) {
            if (ssTable.tryOpen()) {
                try (ssTable) {
                    if (!ssTable.isAlive().get()) {
                        continue;
                    }
                    final long offset = SSTableUtil.binarySearch(key, ssTable, comparator);
                    if (offset >= 0) {
                        return SSTableUtil.readBlock(ssTable, offset);
                    }
                }
            }
        }
        return null;
    }

    @Override
    public void flush(final Iterable<Entry<MemorySegment>> entries) throws IOException {
        storage.add(save(entries));
    }

    /**
     * Saves entries with one byte block per element.
     * One block is:
     * [bytes] key [bytes] value.
     * One meta block is :
     * [JAVA_LONG_UNALIGNED] key_offset [JAVA_LONG_UNALIGNED] value_offset
     * SSTable structure:
     * meta block 1
     * meta block 2
     * ...
     * meta block n
     * block 1
     * block 2
     * ...
     * block n
     */
    public String save(
            final Iterable<Entry<MemorySegment>> entries
    ) throws IOException {
        Objects.requireNonNull(entries, "entries must be not null");
        long appendSize = 0;
        long count = 0;
        for (final Entry<MemorySegment> entry : entries) {
            count++;
            appendSize += entry.key().byteSize();
            final MemorySegment value = entry.value();
            if (value != null) {
                appendSize += value.byteSize();
            }
        }
        if (count == 0) {
            return null;
        }
        final long offsetsPartSize = count * Long.BYTES * 2;
        appendSize += offsetsPartSize;
        final Path filePath = newSsTablePath();
        final Path tmpFilePath = filePath.resolveSibling(filePath.getFileName() + ".tmp");
        try (Arena savingArena = Arena.ofConfined()) {
            final MemorySegment mappedSsTable;
            try (FileChannel channel = FileChannel.open(
                    tmpFilePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE)
            ) {
                mappedSsTable = channel.map(FileChannel.MapMode.READ_WRITE, 0, appendSize, savingArena);
            }
            long indexOffset = 0;
            long blockOffset = offsetsPartSize;
            for (final Entry<MemorySegment> entry : entries) {
                mappedSsTable.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, blockOffset);
                indexOffset += Long.BYTES;
                final MemorySegment key = entry.key();
                final long keySize = key.byteSize();
                MemorySegment.copy(key, 0, mappedSsTable, blockOffset, keySize);
                final MemorySegment value = entry.value();
                blockOffset += keySize;
                if (value == null) {
                    mappedSsTable.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, SSTableUtil.tombstone(blockOffset));
                } else {
                    mappedSsTable.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, blockOffset);
                    final long valueSize = value.byteSize();
                    MemorySegment.copy(value, 0, mappedSsTable, blockOffset, valueSize);
                    blockOffset += valueSize;
                }
                indexOffset += Long.BYTES;
            }
        }
        Files.move(
                tmpFilePath,
                filePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
        return filePath.getFileName().toString();
    }

    private Path newSsTablePath() {
        return basePath.resolve(UUID.randomUUID().toString());
    }

    private List<Iterator<Entry<MemorySegment>>> get(
            final MemorySegment from,
            final MemorySegment to,
            final Iterable<SSTable> iterable
    ) {
        final List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        final SSTableGroup group = new SSTableGroup();
        for (final SSTable ssTable : iterable) {
            if (ssTable.tryOpen()) {
                try (ssTable) {
                    iterators.add(get(group, from, to, ssTable));
                }
            }
        }
        return iterators;
    }

    private Iterator<Entry<MemorySegment>> get(
            final SSTableGroup group,
            final MemorySegment from,
            final MemorySegment to,
            final SSTable ssTable
    ) {
        return new SSTableIterator(
                group, ssTable, from, to, storage, comparator
        );
    }

    @Override
    public List<Iterator<Entry<MemorySegment>>> get(final MemorySegment from, final MemorySegment to) {
        return get(from, to, storage);
    }

    @Override
    public void compact() throws IOException {
        final List<SSTable> ssTables = new ArrayList<>();
        storage.iterator().forEachRemaining(ssTables::add);
        final int size = ssTables.size();
        if (size == 0 || size == 1) {
            return;
        }
        final String compactionFileName = save(() -> {
            final MergeIterator.Builder<MemorySegment, Entry<MemorySegment>> builder
                    = new MergeIterator.Builder<>(comparator);
            final SSTableGroup group = new SSTableGroup();
            for (int i = 0; i < ssTables.size(); i++) {
                builder.addIterator(new WrappedIterator<>(i, get(group, null, null, ssTables.get(i))));
            }
            return builder.build();
        });
        storage.compact(compactionFileName, ssTables);
    }

    @Override
    public void close() {
        compactor.close();
        storage.close();
    }
}
