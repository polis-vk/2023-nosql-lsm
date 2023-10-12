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

// TODO: метод read должен уметь искать сразу по всем values файлам
public class FileDao implements Closeable {
    // Файл со значениями
    private static final String VALUES_FILENAME_PREFIX = "values";
    // Файл с метаданными для значений (нужно для бинарного поиска), а также для хранения tombstone
    private static final String OFFSETS_FILENAME_PREFIX = "metadata";
    private final Path writeValuesFilePath;
    private final Path writeOffsetsFilePath;
    private final MemorySegmentComparator comparator;
    private final MemorySegment readValuesMS;
    private final MemorySegment readOffsetsMS;
    private final Arena readArena;
    private long countOfMemorySegments;

    public FileDao(Config config) throws IOException {
        String writeValuesFileName = FileUtils.getNewFileName(config.basePath(), VALUES_FILENAME_PREFIX);
        String writeOffsetsFileName = FileUtils.getNewFileName(config.basePath(), OFFSETS_FILENAME_PREFIX);
        writeValuesFilePath = config.basePath().resolve(writeValuesFileName);
        writeOffsetsFilePath = config.basePath().resolve(writeOffsetsFileName);

        if (Files.notExists(writeValuesFilePath) || Files.notExists(writeOffsetsFilePath)) {
            comparator = null;
            readArena = null;
            readValuesMS = null;
            readOffsetsMS = null;
            return;
        }

        comparator = new MemorySegmentComparator();
        readArena = Arena.ofShared();

        // Iterator<MemorySegment> valuesIterators = FileIterator.createMany(config.basePath(), VALUES_FILENAME_PREFIX);
        // Iterator<MemorySegment> offsetsIterators = FileIterator.createMany(config.basePath(), OFFSETS_FILENAME_PREFIX);

        try (FileChannel valuesChannel = FileChannel.open(writeValuesFilePath, StandardOpenOption.READ);
             FileChannel offsetsChannel = FileChannel.open(writeOffsetsFilePath, StandardOpenOption.READ)) {
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

        long keyValuePairOffset = Utils.binarySearch(readValuesMS, readOffsetsMS, msKey, comparator);
        if (keyValuePairOffset == -1) {
            return null;
        }

        long keySizeOffset = readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, keyValuePairOffset);
        MemorySegment key = Utils.getBySizeOffset(readValuesMS, keySizeOffset);
        long valueSizeOffset = keySizeOffset + Long.BYTES + key.byteSize();
        MemorySegment value = Utils.getBySizeOffset(readValuesMS, valueSizeOffset);

        return new BaseEntry<>(
                MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)),
                MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)));
    }

    void write(InMemoryDao inMemoryDao) throws IOException {
        try (FileChannel valuesChannel = FileChannel.open(
                writeValuesFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel offsetsChannel = FileChannel.open(
                     writeOffsetsFilePath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
            try (Arena arena = Arena.ofConfined()) {
                long valuesFileOffset = 0L;
                long offsetsFileOffset = 0L;
                MemorySegment valuesStorage = getValuesStorage(inMemoryDao.all(), valuesChannel, arena);
                MemorySegment offsetsStorage = getOffsetsStorage(offsetsChannel, arena);

                Iterator<Entry<MemorySegment>> it = inMemoryDao.all();
                while (it.hasNext()) {
                    Entry<MemorySegment> keyValuePair = it.next();
                    offsetsFileOffset = Utils.writeOffset(valuesFileOffset, offsetsStorage, offsetsFileOffset);
                    valuesFileOffset = Utils.writeKeyValuePair(keyValuePair, valuesStorage, valuesFileOffset);
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
