package ru.vk.itmo.solonetsarseniy.helpers;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.solonetsarseniy.exception.DaoException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentSkipListMap;

import static java.lang.foreign.ValueLayout.JAVA_LONG_UNALIGNED;
import static java.nio.channels.FileChannel.MapMode.READ_ONLY;
import static java.nio.channels.FileChannel.MapMode.READ_WRITE;
import static ru.vk.itmo.solonetsarseniy.exception.DaoExceptions.ERROR_READING_DATA;

public class DataStorageManager {
    private static final String DATA_FILE_NAME = "storage";
    private static final long LONG_SIZE = 8L;
    private static final StandardOpenOption[] WRITE_OPTIONS_KIT = new StandardOpenOption[] {
        StandardOpenOption.CREATE,
        StandardOpenOption.WRITE,
        StandardOpenOption.READ,
        StandardOpenOption.TRUNCATE_EXISTING
    };
    private static final StandardOpenOption[] READ_OPTIONS_KIT = new StandardOpenOption[] {
        StandardOpenOption.READ
    };

    private MemorySegment data;
    private final Config config;
    private final Arena arena = Arena.ofConfined();
    private final MemorySegmentComparator comparator = new MemorySegmentComparator();
    private final Path path;

    public DataStorageManager(Config config) {
        this.config = config;
        this.path = getPath();

        if (Files.exists(path)) {
            try (FileChannel dataChannel = FileChannel.open(path, READ_OPTIONS_KIT)) {
                data = readMemorySegment(dataChannel, path);
            } catch (IOException e) {
                throw new DaoException(ERROR_READING_DATA.getErrorString(), e);
            }
        } else {
            data = null;
        }

    }

    public Entry<MemorySegment> get(MemorySegment key) {
        if (data == null) {
            return null;
        }

        long offset = 0L;
        long fileSize = data.byteSize();
        if (fileSize < JAVA_LONG_UNALIGNED.byteSize()) {
            return null;
        }
        while (offset < fileSize) {
            long keySize = data.get(JAVA_LONG_UNALIGNED, offset);
            offset += LONG_SIZE;
            if (fileSize < offset) {
                return null;
            }
            final MemorySegment storedKey = data.asSlice(offset, keySize);
            offset += keySize;
            if (fileSize < offset) {
                return null;
            }
            long valueSize = data.get(JAVA_LONG_UNALIGNED, offset);
            offset += LONG_SIZE;

            if (fileSize < offset) {
                return null;
            }
            if (comparator.compare(key, storedKey) == 0) {
                MemorySegment value = data.asSlice(offset, valueSize);
                return new BaseEntry<>(storedKey, value);
            }

            offset += valueSize;
        }

        return null;
    }

    public void flush(
        ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> database
    ) throws IOException {
        try (
            FileChannel dataChannel = FileChannel.open(path, WRITE_OPTIONS_KIT)
        ) {
            data = createMemorySegment(dataChannel, countDataSize(database));
            long dataPointer = 0L;
            for (var entry : database.values()) {
                dataPointer += writeEntry(data, dataPointer, entry);
            }
        }
    }

    private long countDataSize(ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> database) {
        return database.values()
            .stream()
            .mapToLong(this::getEntrySize)
            .sum();
    }

    private long getEntrySize(Entry<MemorySegment> entry) {
        long keySize = entry.key().byteSize();
        long valueSize = entry.value().byteSize();
        return keySize
            + valueSize
            + (2L * LONG_SIZE);
    }

    private Path getPath() {
        Path pathToDataFile = Path.of(DATA_FILE_NAME);
        return config.basePath().resolve(pathToDataFile);
    }

    private MemorySegment createMemorySegment(
        FileChannel channel,
        long dataSize
    ) throws IOException {
        return channel.map(
            READ_WRITE,
            0,
            dataSize,
            arena
        );
    }

    private MemorySegment readMemorySegment(
        FileChannel channel,
        Path path
    ) throws IOException {
        return channel.map(
            READ_ONLY,
            0,
            Files.size(path),
            arena
        );
    }

    private long writeEntry(
        MemorySegment dataMemorySegment,
        long dataPointer,
        Entry<MemorySegment> entry
    ) {
        MemorySegment key = entry.key();
        long localOffset = writeMemorySegment(key, dataMemorySegment, dataPointer);
        MemorySegment value = entry.value();
        return localOffset + writeMemorySegment(
            value,
            dataMemorySegment,
            dataPointer + localOffset
        );
    }

    private long writeMemorySegment(
        MemorySegment segment,
        MemorySegment dataMemorySegment,
        long dataPointer
    ) {
        long keySize = segment.byteSize();
        dataMemorySegment.set(JAVA_LONG_UNALIGNED, dataPointer, keySize);
        dataMemorySegment.asSlice(dataPointer + Long.BYTES, keySize)
            .copyFrom(segment);
        return (keySize + LONG_SIZE);
    }
}
