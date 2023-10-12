package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class FileDao implements Closeable {
    // Файл со значениями
    private static final String VALUES_FILENAME = "values";
    // Файл с оффсетами для значений (нужно для бинарного поиска)
    private static final String OFFSETS_FILENAME = "offsets";
    private final Path valuesPath;
    private final Path offsetsPath;
    private final MemorySegmentComparator comparator;
    private final MemorySegment readValuesMS;
    private final MemorySegment readOffsetsMS;
    private final Arena readArena;
    private long countOfMemorySegments;

    public FileDao(Config config) throws IOException {
        var valuesFileName = FileHelper.getNewFileName(config.basePath(), VALUES_FILENAME);
        var offsetsFileName = FileHelper.getNewFileName(config.basePath(), OFFSETS_FILENAME);
        valuesPath = config.basePath().resolve(valuesFileName);
        offsetsPath = config.basePath().resolve(offsetsFileName);
        comparator = new MemorySegmentComparator();

        if (Files.notExists(valuesPath) || Files.notExists(offsetsPath)) {
            readValuesMS = null;
            readOffsetsMS = null;
            readArena = null;
            return;
        }

        readArena = Arena.ofShared();
        try (FileChannel valuesChannel = FileChannel.open(valuesPath, StandardOpenOption.READ);
             FileChannel offsetsChannel = FileChannel.open(offsetsPath, StandardOpenOption.READ)) {
            readValuesMS = valuesChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    valuesChannel.size(), readArena);
            readOffsetsMS = offsetsChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    offsetsChannel.size(), readArena);
        }
    }

    Entry<MemorySegment> read(MemorySegment msKey) {
        if (readValuesMS == null || readOffsetsMS == null) {
            return null;
        }

        var keyValuePairOffset = leftBinarySearch(readValuesMS, readOffsetsMS, msKey);
        if (keyValuePairOffset == -1) {
            return null;
        }

        long keySizeOffset = readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, keyValuePairOffset);
        MemorySegment key = getBySizeOffset(readValuesMS, keySizeOffset);
        long valueSizeOffset = keySizeOffset + Long.BYTES + key.byteSize();
        MemorySegment value = getBySizeOffset(readValuesMS, valueSizeOffset);

        return new BaseEntry<>(
                MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)),
                MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)));
    }

    private MemorySegment getBySizeOffset(MemorySegment valuesStorage, long sizeOffset) {
        long valueSize = valuesStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, sizeOffset);
        long valueOffset = sizeOffset + Long.BYTES;
        return valuesStorage.asSlice(valueOffset, valueSize);
    }

    // Данный бинарный поиск нужен для нахождения первого ключа в файле, который больше либо равен нужному ключу
    private long leftBinarySearch(MemorySegment valuesStorage, MemorySegment offsetsStorage, MemorySegment desiredKey) {
        long offsetsCount = offsetsStorage.byteSize() / Long.BYTES;
        long l = 0;
        long r = offsetsCount - 1;

        while (l < r) {
            long m = l + (r - l) / 2;

            long keySizeOffset = offsetsStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, m * Long.BYTES);
            MemorySegment key = getBySizeOffset(valuesStorage, keySizeOffset);

            if (comparator.compare(key, desiredKey) == 0) {
                return m * Long.BYTES;
            } else if (comparator.compare(key, desiredKey) < 0) {
                l = m + 1;
            } else {
                r = m;
            }
        }

        // Если найденный ключ оказался меньше нужного, то мы говорим, что ничего не нашли
        long keySizeOffset = offsetsStorage.get(ValueLayout.JAVA_LONG_UNALIGNED, l * Long.BYTES);
        MemorySegment key = getBySizeOffset(valuesStorage, keySizeOffset);
        if (key != null && comparator.compare(key, desiredKey) < 0) {
            return -1;
        }

        return l * Long.BYTES;
    }

    void write(InMemoryDao inMemoryDao) throws IOException {
        try (FileChannel valuesChannel = FileChannel.open(
                valuesPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel offsetsChannel = FileChannel.open(
                     offsetsPath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
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

    @Override
    public void close() throws IOException {
        if (readArena != null) {
            readArena.close();
        }
    }

    /*
     Пример: у нас есть три файла со следующим содержимым
     |k1 k2| |k0 k2 k4| |k1 k3|
     И данные в буфере
     |k2 k5|

     Создаем 4 итератора, по одному на каждый файл и один на буфер
     Теперь двигаем каждый итератор к максимальному элементу больше from или ровно на from в файле
     Шаг алгоритма:
     1)
     Номер итератора в файле: ключ
     1: k1
     2: k2
     3: k1
     4: k2

     Сортируем значения итераторов между собой по значению, а потом по номеру итератора
     Сортированные значения:
     (3)k1 (1)k1 (4)k2 (2)k2
     Возвращаем первый минимальный ключ у максимального итератора
     Результирующий итератор: (3)k1
     2)
     Двигаем итераторы c найденным значением вправо
     1: k1 -> k2
     2: k2
     3: k1 -> k3
     4: k2
     Опять сортируем по значению и находим итератор более свежего файла
     (4)k2 (2)k2 (1)k2 (3)k3
     Понимаем, что нам нужен (4)k2
     Результирующий итератор: (3)k1 (4)k2
     3)
     Двигаем итераторы c найденным значением вправо
     1: k2 -> end
     2: k2 -> k4
     3: k3
     4: k2 -> k5
     Опять сортируем по значению и находим итератор более свежего файла
     (3)k3 (2)k4 (4)k5
     Понимаем, что нам нужен (3)k3
     Результирующий итератор: (3)k1 (4)k2 (3)k3
     4)
     Двигаем итераторы c найденным значением вправо
     1: end
     2: k4
     3: k3 -> end
     4: k5
     Опять сортируем по значению и находим итератор более свежего файла
     (2)k4 (4)k5
     Понимаем, что нам нужен (2)k4
     Результирующий итератор: (3)k1 (4)k2 (3)k3 (2)k4
     5)
     Двигаем итераторы c найденным значением вправо
     1: end
     2: k4 -> end
     3: end
     4: k5
     Опять сортируем по значению и находим итератор более свежего файла
     (4)k5
     Понимаем, что нам нужен (4)k5
     Результирующий итератор: (3)k1 (4)k2 (3)k3 (2)k4 (4)k5
     6)
     Двигаем итераторы c найденным значением вправо
     1: end
     2: end
     3: end
     4: k5 -> end
     Понимаем, что все итераторы дошли до конца, ничего не возвращаем
     Результирующий итератор: (3)k1 (4)k2 (3)k3 (2)k4 (4)k5
    */
}
