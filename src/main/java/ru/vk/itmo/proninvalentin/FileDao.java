package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.comparators.MemorySegmentComparator;
import ru.vk.itmo.proninvalentin.iterators.FileIterator;
import ru.vk.itmo.proninvalentin.iterators.PeekingPriorityIterator;
import ru.vk.itmo.proninvalentin.iterators.PeekingPriorityIteratorImpl;
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
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
    // Префикс файлов с оффсетами каждой entry
    public static final String OFFSETS_FILENAME_PREFIX = "offsets";
    private final MemorySegmentComparator comparator;
    // Список замапленных MS для чтения файлов со значениями, начиная с самых новых файлов и заканчивая самыми старыми
    private final List<MemorySegment> readValuesMSStorage;
    // Список замапленных MS для чтения файлов с оффсетами, начиная с самых новых файлов и заканчивая самыми старыми
    private final List<MemorySegment> readOffsetsMSStorage;
    private final Arena readArena;
    private final Path basePath;
    private long countOfMemorySegments;

    public FileDao(Config config) throws IOException {
        basePath = config.basePath();

        if (Files.notExists(basePath)) {
            comparator = null;
            readArena = null;
            readValuesMSStorage = Collections.emptyList();
            readOffsetsMSStorage = Collections.emptyList();
            return;
        }

        comparator = MemorySegmentComparator.getInstance();
        readArena = Arena.ofShared();
        readValuesMSStorage = new ArrayList<>();
        readOffsetsMSStorage = new ArrayList<>();
        initReadMSLists();
    }

    // Пройтись по всем парам файлов (оффсеты + значения) в basePath и замапить MemorySegments для них
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
            } else if (file.getName().startsWith(OFFSETS_FILENAME_PREFIX)) {
                readOffsetsMSStorage.add(getReadOnlyMappedMemory(file));
            }
        }

        if (readValuesMSStorage.size() != readOffsetsMSStorage.size()) {
            throw new IllegalArgumentException(
                    "Directory in config must contain same number of files with prefix: \"%s\" and \"%s\""
                            .formatted(VALUES_FILENAME_PREFIX, OFFSETS_FILENAME_PREFIX));
        }
    }

    public List<PeekingPriorityIterator> getFileIterators(MemorySegment from, MemorySegment to) {
        List<PeekingPriorityIterator> filesIterators = new ArrayList<>();

        for (int i = readValuesMSStorage.size() - 1, priority = 1; i >= 0; i--, priority++) {
            filesIterators.add(new PeekingPriorityIteratorImpl(new FileIterator(
                    readValuesMSStorage.get(i),
                    readOffsetsMSStorage.get(i),
                    from,
                    to),
                    priority));
        }

        return filesIterators;
    }

    // Замапить файл в MemorySegment для чтения
    private MemorySegment getReadOnlyMappedMemory(File file) throws IOException {
        try (FileChannel offsetsChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            return offsetsChannel.map(FileChannel.MapMode.READ_ONLY, 0, offsetsChannel.size(), readArena);
        }
    }

    // Найти указанный ключ во всех файлах со значениями
    Entry<MemorySegment> read(MemorySegment msKey) {
        for (int i = readValuesMSStorage.size() - 1; i >= 0; i--) {
            MemorySegment readValuesMS = readValuesMSStorage.get(i);
            MemorySegment readOffsetsMS = readOffsetsMSStorage.get(i);

            long entryIndex = MemorySegmentUtils.binarySearch(readValuesMS, readOffsetsMS, msKey, comparator);
            if (entryIndex != -1) {
                return MemorySegmentUtils.getEntryByIndex(readValuesMS, readOffsetsMS, entryIndex);
            }
        }

        return null;
    }

    // Пройтись по всем парам замапленных MS и создать для каждого итератор
    void write(InMemoryDao inMemoryDao) throws IOException {
        if (!inMemoryDao.all().hasNext()) {
            return;
        }

        Path tmpWriteValuesFilePath = basePath.resolve("tmp_values");
        Path tmpWriteOffsetsFilePath = basePath.resolve("tmp_offsets");
        String writeValuesFileName = FileUtils.getNewFileName(basePath, VALUES_FILENAME_PREFIX);
        String writeOffsetsFileName = FileUtils.getNewFileName(basePath, OFFSETS_FILENAME_PREFIX);
        Path writeValuesFilePath = basePath.resolve(writeValuesFileName);
        Path writeOffsetsFilePath = basePath.resolve(writeOffsetsFileName);

        try (FileChannel valuesChannel = FileChannel.open(
                tmpWriteValuesFilePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING);
             FileChannel offsetsChannel = FileChannel.open(
                     tmpWriteOffsetsFilePath,
                     StandardOpenOption.CREATE,
                     StandardOpenOption.READ,
                     StandardOpenOption.WRITE,
                     StandardOpenOption.TRUNCATE_EXISTING)) {
            try (Arena arena = Arena.ofConfined()) {
                long valueOffset = 0L;
                long entryOffset = 0L;
                MemorySegment valuesMS = getValuesMS(inMemoryDao.all(), valuesChannel, arena);
                MemorySegment offsetsMS = getOffsetsMS(offsetsChannel, arena);

                Iterator<Entry<MemorySegment>> it = inMemoryDao.all();
                while (it.hasNext()) {
                    Entry<MemorySegment> entry = it.next();
                    entryOffset = MemorySegmentUtils.writeEntryOffset(valueOffset, offsetsMS, entryOffset);
                    valueOffset = MemorySegmentUtils.getEntryOffset(entry, valueOffset);
                }

                it = inMemoryDao.all();
                valueOffset = 0L;
                while (it.hasNext()) {
                    Entry<MemorySegment> entry = it.next();
                    valueOffset = MemorySegmentUtils.writeEntry(entry, valuesMS, valueOffset);
                }
            }
        }
        Files.move(
                tmpWriteValuesFilePath,
                writeValuesFilePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
        Files.move(
                tmpWriteOffsetsFilePath,
                writeOffsetsFilePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
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

    private MemorySegment getOffsetsMS(FileChannel offsetsChannel,
                                       Arena arena) throws IOException {
        long inMemoryDataSize = Long.BYTES * countOfMemorySegments;
        return offsetsChannel.map(FileChannel.MapMode.READ_WRITE, 0, inMemoryDataSize, arena);
    }

    @Override
    public void close() throws IOException {
        if (readArena != null) {
            readArena.close();
        }
    }
}
