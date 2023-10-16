package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.comparators.CreateAtTimeComparator;
import ru.vk.itmo.proninvalentin.comparators.MemorySegmentComparator;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class FileDao implements Closeable {
    // Префикс файлов, в которых хранятся Entry
    public static final String VALUES_FILENAME_PREFIX = "values";
    // Префикс файлов с метаданными каждой entry
    // Метаданные выглядят следующим образом: |Оффсет entry в файле со значениями|Tombstone бит|
    // Tombstone бит служит для указания информации о том удалена ли запись или нет
    public static final String METADATA_FILENAME_PREFIX = "metadata";
    private final MemorySegmentComparator comparator;
    // Список замапленных MS для чтения файлов со значениями, начиная с самых новых файлов и заканчивая самыми старыми
    private final List<MemorySegment> readValuesMSStorage;
    // Список замапленных MS для чтения файлов с метаданными, начиная с самых новых файлов и заканчивая самыми старыми
    private final List<MemorySegment> readMetadataMSStorage;
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
            readValuesMSStorage = Collections.emptyList();
            readMetadataMSStorage = Collections.emptyList();
            metadataFiles = Collections.emptyList();
            return;
        }

        comparator = new MemorySegmentComparator();
        readArena = Arena.ofShared();
        readValuesMSStorage = new ArrayList<>();
        readMetadataMSStorage = new ArrayList<>();
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
                .sorted(Comparator.comparing(x -> FileUtils.parseIndexFromFileName(x.getName())))
                .collect(Collectors.toList())) {
            if (file.getName().startsWith(VALUES_FILENAME_PREFIX)) {
                readValuesMSStorage.add(getReadOnlyMappedMemory(file));
            } else if (file.getName().startsWith(METADATA_FILENAME_PREFIX)) {
                metadataFiles.add(file);
                readMetadataMSStorage.add(getReadOnlyMappedMemory(file));
            }
        }

        if (readValuesMSStorage.size() != readMetadataMSStorage.size()) {
            throw new IllegalArgumentException(
                    "Directory in config must contain same number of files with name: \"%s\" and \"%s\""
                            .formatted(VALUES_FILENAME_PREFIX, METADATA_FILENAME_PREFIX));
        }
    }

    // Замапить файл в MemorySegment для чтения
    private MemorySegment getReadOnlyMappedMemory(File file) throws IOException {
        try (FileChannel metadataChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            return metadataChannel.map(FileChannel.MapMode.READ_ONLY, 0, metadataChannel.size(), readArena);
        }
    }

    // Найти указанный ключ во всех файлах со значениями
    EnrichedEntry read(MemorySegment msKey) {
        List<EnrichedEntry> entries = new ArrayList<>();
        for (int i = 0; i < readValuesMSStorage.size(); i++) {
            MemorySegment readValuesMS = readValuesMSStorage.get(i);
            MemorySegment readMetadataMS = readMetadataMSStorage.get(i);

            long valueIndex = MemorySegmentUtils.binarySearch(readValuesMS, readMetadataMS, msKey, comparator);
            if (valueIndex != -1) {
                entries.add(new EnrichedEntry(
                        MemorySegmentUtils.getMetadataByIndex(readMetadataMS, valueIndex),
                        MemorySegmentUtils.getEntryByIndex(readValuesMS, readMetadataMS, valueIndex)));
            }
        }

        if (entries.isEmpty()) {
            return null;
        } else {
            Comparator<EnrichedEntry> enrichedEntryComparator = Comparator.comparing(x ->
                    x.metadata.createdAt, new CreateAtTimeComparator());
            EnrichedEntry entry = entries.stream().sorted(enrichedEntryComparator).toList().getFirst();
            if (entry.metadata.isDeleted) {
                return null;
            } else {
                return entry;
            }
        }
    }

    // Пройтись по всем парам замапленных MS и создать для каждого итератор
    void write(InMemoryDao inMemoryDao) throws IOException {
        if (!inMemoryDao.all().hasNext()) {
            return;
        }
        String writeValuesFileName = FileUtils.getNewFileName(basePath, VALUES_FILENAME_PREFIX);
        String writeMetadataFileName = FileUtils.getNewFileName(basePath, METADATA_FILENAME_PREFIX);
        Path writeValuesFilePath = basePath.resolve(writeValuesFileName);
        Path writeMetadataFilePath = basePath.resolve(writeMetadataFileName);

        try (FileChannel valuesChannel = FileChannel.open(
                writeValuesFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel metadataChannel = FileChannel.open(
                     writeMetadataFilePath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
            try (Arena arena = Arena.ofConfined()) {
                long valueOffset = 0L;
                long metadataOffset = 0L;
                MemorySegment valuesMS = getValuesMS(inMemoryDao.all(), valuesChannel, arena);
                MemorySegment metadataMS = getMetadataMS(metadataChannel, arena);

                Iterator<Entry<MemorySegment>> it = inMemoryDao.all();
                long createdAt = Instant.now().toEpochMilli() + metadataFiles.size();
                while (it.hasNext()) {
                    Entry<MemorySegment> entry = it.next();
                    metadataOffset = MemorySegmentUtils.writeEntryMetadata(
                            valueOffset, entry.value() == null, createdAt,
                            metadataMS, metadataOffset);
                    valueOffset = MemorySegmentUtils.writeEntry(entry, valuesMS, valueOffset);
                }
            }
        }
    }

    // Получить список итераторов по файлам. Ближе к началу находятся итераторы с более свежими данными
    public List<Iterator<EnrichedEntry>> getFilesIterators(MemorySegment from,
                                                           MemorySegment to) throws IOException {
        List<Iterator<EnrichedEntry>> filesIterators = new ArrayList<>(readValuesMSStorage.size());

        for (int i = 0; i < readValuesMSStorage.size(); i++) {
            filesIterators.add(FileIterator.create(
                    readValuesMSStorage.get(i),
                    readMetadataMSStorage.get(i),
                    from,
                    to,
                    comparator));
        }

        return filesIterators;
    }

    private MemorySegment getValuesMS(Iterator<Entry<MemorySegment>> valuesInMemory,
                                      FileChannel valuesChannel,
                                      Arena arena) throws IOException {
        long keysSize = 0L;
        long valuesSize = 0L;

        while (valuesInMemory.hasNext()) {
            Entry<MemorySegment> v = valuesInMemory.next();
            keysSize += v.key().byteSize();
            if (v.value() != null) {
                valuesSize += v.value().byteSize();
            }
            countOfMemorySegments++;
        }

        long inMemoryDataSize = 2L * Long.BYTES * countOfMemorySegments + keysSize + valuesSize;
        return valuesChannel.map(FileChannel.MapMode.READ_WRITE, 0, inMemoryDataSize, arena);
    }

    private MemorySegment getMetadataMS(FileChannel metadataChannel,
                                        Arena arena) throws IOException {
        long inMemoryDataSize = Metadata.SIZE * countOfMemorySegments;
        return metadataChannel.map(FileChannel.MapMode.READ_WRITE, 0, inMemoryDataSize, arena);
    }

    @Override
    public void close() throws IOException {
        if (readArena != null) {
            readArena.close();
        }
    }
}
