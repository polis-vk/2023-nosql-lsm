package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.iterators.FileIterator;
import ru.vk.itmo.proninvalentin.utils.FileUtils;
import ru.vk.itmo.proninvalentin.utils.MemorySegmentUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.*;

// TODO: проверить, чтобы у всех методов был package-private или другой любой минимальный модификатор доступа
// TODO: переназвать все offsets на metadata
// TODO: переназвать все ms на storage
// TODO: убрать все var
// TODO: сохранение entry со значением null должно выставлять Tombstone бит в 1
// TODO: заменить все LONG.BYTES на константы (не должно остаться ни одного Long.Bytes)
public class FileDao implements Closeable {
    // Префикс файлов, в которых хранятся Entry
    public static final String VALUES_FILENAME_PREFIX = "values";
    // Префикс файлов с метаданными каждой entry
    // Метаданные выглядят следующим образом: |Оффсет entry в файле со значениями|Tombstone бит|
    // Tombstone бит служит для указания информации о том удалена ли запись или нет
    public static final String OFFSETS_FILENAME_PREFIX = "offsets";
    private final MemorySegmentComparator comparator;
    // Список замапленных MS для чтения файлов со значениями, начиная с самых новых файлов и заканчивая самыми старыми
    private final List<MemorySegment> readValuesStorages;
    // Список замапленных MS для чтения файлов с метаданными, начиная с самых новых файлов и заканчивая самыми старыми
    private final List<MemorySegment> readOffsetsStorages;
    private final Arena readArena;
    private final Path basePath;
    private long countOfMemorySegments;
    // Список файлов с метаданными, нужен для создания итераторов по метаданным
    private final List<File> metadataFiles;

    public FileDao(Config config) throws IOException {
        basePath = config.basePath();

        if (Files.notExists(basePath)) {
            comparator = null;
            readArena = null;
            readValuesStorages = Collections.emptyList();
            readOffsetsStorages = Collections.emptyList();
            metadataFiles = Collections.emptyList();
            return;
        }

        comparator = new MemorySegmentComparator();
        readArena = Arena.ofShared();
        readValuesStorages = new ArrayList<>();
        readOffsetsStorages = new ArrayList<>();
        metadataFiles = new ArrayList<>();
        initReadMSLists();
    }

    // Пройтись по всем парам файлов (метаданные + значения) в basePath и замапить MemorySegments для них
    private void initReadMSLists() throws IOException {
        File folder = new File(basePath.toUri());
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return;
        }

        for (File file : Arrays.stream(listOfFiles)
                .filter(File::isFile)
                .sorted(Comparator.comparing(File::getName))
                .sorted(Collections.reverseOrder())
                .toList()) {
            if (file.getName().startsWith(VALUES_FILENAME_PREFIX)) {
                readValuesStorages.add(getReadOnlyMappedMemory(file));
            } else if (file.getName().startsWith(OFFSETS_FILENAME_PREFIX)) {
                metadataFiles.add(file);
                readOffsetsStorages.add(getReadOnlyMappedMemory(file));
            }
        }

        if (readValuesStorages.size() != readOffsetsStorages.size()) {
            throw new IllegalArgumentException(
                    "Directory in config must contain same number of files with name: \"%s\" and \"%s\""
                            .formatted(VALUES_FILENAME_PREFIX, OFFSETS_FILENAME_PREFIX));
        }
    }

    // Замапить файл в MemorySegment для чтения
    private MemorySegment getReadOnlyMappedMemory(File file) throws IOException {
        try (FileChannel offsetsChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            return offsetsChannel.map(FileChannel.MapMode.READ_ONLY, 0, offsetsChannel.size(), readArena);
        }
    }

    // Найти указанный ключ во всех файлах со значениями
    Entry<MemorySegment> read(MemorySegment msKey) {
        for (int i = 0; i < readValuesStorages.size(); i++) {
            var readValuesMS = readValuesStorages.get(i);
            var readOffsetsMS = readOffsetsStorages.get(i);
            long valueIndex = MemorySegmentUtils.binarySearch(readValuesMS, readOffsetsMS, msKey, comparator);
            if (valueIndex != -1) {
                return MemorySegmentUtils.getEntryByIndex(readValuesMS, readOffsetsMS, valueIndex);
            }
        }

        return null;
    }

    // Пройтись по всем парам замапленных MS и создать для каждого итератор
    void write(InMemoryDao inMemoryDao) throws IOException {
        String writeValuesFileName = FileUtils.getNewFileName(basePath, VALUES_FILENAME_PREFIX);
        String writeOffsetsFileName = FileUtils.getNewFileName(basePath, OFFSETS_FILENAME_PREFIX);
        Path writeValuesFilePath = basePath.resolve(writeValuesFileName);
        Path writeOffsetsFilePath = basePath.resolve(writeOffsetsFileName);

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
                long valueOffset = 0L;
                long metadataOffset = 0L;
                MemorySegment valuesStorage = getValuesStorage(inMemoryDao.all(), valuesChannel, arena);
                MemorySegment offsetsStorage = getOffsetsStorage(offsetsChannel, arena);

                Iterator<Entry<MemorySegment>> it = inMemoryDao.all();
                while (it.hasNext()) {
                    Entry<MemorySegment> entry = it.next();
                    metadataOffset = MemorySegmentUtils.writeEntryMetadata(
                            valueOffset, entry.value() == null, Instant.now().toEpochMilli(),
                            offsetsStorage, metadataOffset);
                    valueOffset = MemorySegmentUtils.writeEntry(entry, valuesStorage, valueOffset);
                }
            }
        }
    }

    // Получить список итераторов по файлам. Ближе к началу находятся итераторы с более свежими данными
    public List<Iterator<Entry<MemorySegment>>> getFilesIterators(MemorySegment from,
                                                                  MemorySegment to) throws IOException {
        List<Iterator<Entry<MemorySegment>>> filesIterators = new ArrayList<>(readValuesStorages.size());

        for (int i = 0; i < readValuesStorages.size(); i++) {
            filesIterators.add(FileIterator.create(
                    readValuesStorages.get(i),
                    readOffsetsStorages.get(i),
                    from,
                    to,
                    comparator,
                    metadataFiles.get(i).length()));
        }

        return filesIterators;
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
        long inMemoryDataSize = Metadata.SIZE * countOfMemorySegments;
        return offsetsChannel.map(FileChannel.MapMode.READ_WRITE, 0, inMemoryDataSize, arena);
    }

    @Override
    public void close() throws IOException {
        if (readArena != null) {
            readArena.close();
        }
    }
}
