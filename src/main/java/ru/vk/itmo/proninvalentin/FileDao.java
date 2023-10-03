package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class FileDao {
    // Файл со значениями
    private static final String VALUES_FILENAME = "values";
    // Файл с оффсетами для значений (нужно для бинарного поиска)
    private static final String OFFSETS_FILENAME = "offsets";
    private final Path valuesPath;
    private final Path offsetsPath;
    private final MemorySegmentComparator comparator;
    private long countOfMemorySegments;

    public FileDao(Config config) {
        valuesPath = config.basePath().resolve(VALUES_FILENAME);
        offsetsPath = config.basePath().resolve(OFFSETS_FILENAME);
        comparator = new MemorySegmentComparator();
    }

    Entry<MemorySegment> read(MemorySegment msKey) {
        if (Files.notExists(valuesPath) || Files.notExists(offsetsPath)) {
            return null;
        }

        try (FileChannel valuesChannel = FileChannel.open(valuesPath, StandardOpenOption.READ);
             FileChannel offsetsChannel = FileChannel.open(offsetsPath, StandardOpenOption.READ)) {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment valuesStorage = valuesChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                        valuesChannel.size(), arena);
                MemorySegment offsetsStorage = offsetsChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                        offsetsChannel.size(), arena);

                var keyValuePairOffset = binarySearch(valuesStorage, offsetsStorage, msKey);
                if (keyValuePairOffset == -1) {
                    return null;
                }

                long keySizeOffset = offsetsStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, keyValuePairOffset);
                MemorySegment key = getBySizeOffset(valuesStorage, keySizeOffset);
                long valueSizeOffset = keySizeOffset + Long.BYTES + key.byteSize();
                MemorySegment value = getBySizeOffset(valuesStorage, valueSizeOffset);

                return new BaseEntry<>(
                        MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)),
                        MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)));
            }
        } catch (IOException e) {
            return null;
        }
    }

    private MemorySegment getBySizeOffset(MemorySegment valuesStorage, long sizeOffset) {
        long valueSize = valuesStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, sizeOffset);
        long valueOffset = sizeOffset + Long.BYTES;
        return valuesStorage.asSlice(valueOffset, valueSize);
    }

    private long binarySearch(MemorySegment valuesStorage, MemorySegment offsetsStorage, MemorySegment desiredKey) {
        long offsetsCount = offsetsStorage.byteSize() / Long.BYTES;
        long l = 0;
        long r = offsetsCount - 1;

        while (l <= r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = offsetsStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Long.BYTES);
            MemorySegment key = getBySizeOffset(valuesStorage, keySizeOffset);

            if (comparator.compare(key, desiredKey) == 0) {
                return m * Long.BYTES;
            } else if (comparator.compare(key, desiredKey) < 0) {
                l = m + 1;
            } else {
                r = m - 1;
            }
        }

        return -1;
    }

    void write(InMemoryDao inMemoryDao) throws IOException {
        try (FileChannel valuesChannel = FileChannel.open(
                valuesPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
             FileChannel offsetsChannel = FileChannel.open(
                     offsetsPath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE)) {
            try (Arena arena = Arena.ofConfined()) {
                long valuesFileOffset = 0L;
                long offsetsFileOffset = 0L;
                MemorySegment valuesStorage = getValuesStorage(inMemoryDao.all(), valuesChannel, arena);
                MemorySegment offsetsStorage = getOffsetsStorage(offsetsChannel, arena);

                var it = inMemoryDao.all();
                while (it.hasNext()) {
                    Entry<MemorySegment> keyValuePair = it.next();
                    offsetsFileOffset = writeOffset(valuesFileOffset, offsetsStorage, offsetsFileOffset);
                    valuesFileOffset = writeKeyValuePair(keyValuePair, valuesStorage, valuesFileOffset);
                }
            }
        }
    }

    private MemorySegment getValuesStorage(Iterator<Entry<MemorySegment>> valuesInMemory,
                                           FileChannel valuesChannel,
                                           Arena arena) throws IOException {
        long keysSize = 0L;
        long valuesSize = 0L;

        while (valuesInMemory.hasNext()) {
            Entry<MemorySegment> v = valuesInMemory.next();
            keysSize += v.key().byteSize();
            valuesSize += v.value().byteSize();
            countOfMemorySegments++;
        }

        long inMemoryDataSize = 2L * Long.BYTES * countOfMemorySegments + keysSize + valuesSize;
        return valuesChannel.map(FileChannel.MapMode.READ_WRITE, 0, inMemoryDataSize, arena);
    }

    private MemorySegment getOffsetsStorage(FileChannel offsetsChannel,
                                            Arena arena) throws IOException {
        long inMemoryDataSize = (long) Long.BYTES * countOfMemorySegments;
        return offsetsChannel.map(FileChannel.MapMode.READ_WRITE, 0, inMemoryDataSize, arena);
    }

    // Пары "ключ:значение" хранятся внутри файла в следующем виде:
    // |Длина ключа в байтах|Ключ|Длина значения в байтах|Значение|
    private long writeKeyValuePair(Entry<MemorySegment> src, MemorySegment dst, final long fileOffset) {
        var localFileOffset = fileOffset;
        // Сначала пишем длину ключа и сам ключ
        localFileOffset = writeMemorySegment(src.key(), dst, localFileOffset);
        // Потом пишем длину значения и само значение
        localFileOffset = writeMemorySegment(src.value(), dst, localFileOffset);
        return localFileOffset;
    }

    // Записать пару: |Длина значения в байтах|Значение|
    private long writeMemorySegment(MemorySegment value, MemorySegment dst, final long fileOffset) {
        long valueSize = value.byteSize();
        long localFileOffset = fileOffset;
        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, localFileOffset, valueSize);
        localFileOffset += Long.BYTES;
        MemorySegment.copy(value, 0, dst, localFileOffset, valueSize);
        localFileOffset += valueSize;
        return localFileOffset;
    }

    // Записать оффсет: |Сдвиг значения в байтах от начала файла|
    private long writeOffset(long offset, MemorySegment dst, long fileOffset) {
        dst.set(ValueLayout.JAVA_LONG_UNALIGNED, fileOffset, offset);
        return fileOffset + Long.BYTES;
    }
}
