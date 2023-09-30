package ru.vk.itmo.smirnovdmitrii;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.OutMemoryDao;
import ru.vk.itmo.smirnovdmitrii.util.MemorySegmentComparator;

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
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

public class FileDao implements OutMemoryDao<MemorySegment, Entry<MemorySegment>> {

    private static final String SS_TABLE_NAME = "ss_table";
    private final Path basePath;

    private final Comparator<MemorySegment> comparator = new MemorySegmentComparator();

    public FileDao(final Path basePath) {
        try {
            Files.createDirectories(basePath);
            final Path filePath = basePath.resolve(SS_TABLE_NAME);
            if (!Files.exists(filePath)) {
                Files.createFile(basePath.resolve(SS_TABLE_NAME));
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        this.basePath = basePath;
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        Objects.requireNonNull(key);
        try (final FileChannel channel = FileChannel.open(basePath.resolve(SS_TABLE_NAME),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ)) {
            final MemorySegment segment = channel.map(
                    FileChannel.MapMode.READ_ONLY, 0, channel.size(), Arena.ofAuto());
            return search(key, segment);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Entry<MemorySegment> search(final MemorySegment key, final MemorySegment storage) {
        long currentPos = 0;
        final long keySize = key.byteSize();
        final long storageSize = storage.byteSize();
        Entry<MemorySegment> result = null;
        while (currentPos < storageSize) {
            final long currentKeySize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, currentPos);
            currentPos += Long.BYTES;
            final MemorySegment currentKey = storage.asSlice(currentPos, currentKeySize);
            currentPos += currentKeySize;
            final long valueSize = storage.get(ValueLayout.JAVA_LONG_UNALIGNED, currentPos);
            currentPos += Long.BYTES;
            if (currentKeySize == keySize) {
                if (comparator.compare(key, currentKey) == 0) {
                    result = new BaseEntry<>(currentKey, storage.asSlice(currentPos, valueSize));
                }
            }
            currentPos += valueSize;
        }
        return result;
    }

    @Override
    public void save(final Map<MemorySegment, Entry<MemorySegment>> storage) {
        Objects.requireNonNull(storage, "storage must be not null");
        if (storage.isEmpty()) {
            return;
        }
        try (final FileChannel channel = FileChannel.open(basePath.resolve(SS_TABLE_NAME),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ)) {
            long appendSize = 0;
            final Collection<Entry<MemorySegment>> values = storage.values();
            for (final Entry<MemorySegment> entry : values) {
                appendSize += entry.value().byteSize() + entry.key().byteSize() + 2L * Long.BYTES;
            }
            final MemorySegment mapped = channel.map(
                    FileChannel.MapMode.READ_WRITE, channel.size(), appendSize, Arena.ofAuto());
            long offset = 0;
            for (final Entry<MemorySegment> entry : values) {
                offset = write(entry.key(), mapped, offset);
                offset = write(entry.value(), mapped, offset);
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private long write(final MemorySegment from, final MemorySegment to, long offset) {
        final long fromByteSize = from.byteSize();
//        if (offset > to.byteSize() - Long.BYTES) {
//            throw new AssertionError();
//        }
        to.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, fromByteSize);
        offset += Long.BYTES;
//        if (offset > to.byteSize() - fromByteSize) {
//            throw new AssertionError();
//        }
        MemorySegment.copy(from, 0, to, offset, fromByteSize);
        return offset + from.byteSize();
    }

}
