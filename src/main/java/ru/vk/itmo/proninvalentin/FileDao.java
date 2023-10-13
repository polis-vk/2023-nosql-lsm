package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

// TODO: метод read должен уметь искать сразу по всем values файлам
// TODO: убрать public у read(from, to)
public class FileDao implements Closeable {
    // Файл со значениями
    private static final String VALUES_FILENAME_PREFIX = "values";
    // Файл с метаданными для значений (нужно для бинарного поиска), а также для хранения tombstone
    private static final String OFFSETS_FILENAME_PREFIX = "offsets";
    private final Path writeValuesFilePath;
    private final Path writeOffsetsFilePath;
    private final MemorySegmentComparator comparator;
    private final List<MemorySegment> readValuesMSList;
    private final List<MemorySegment> readOffsetsMSList;
    private final Arena readArena;
    private long countOfMemorySegments;
    private final List<File> offsetFiles;

    public FileDao(Config config) throws IOException {
        String writeValuesFileName = FileUtils.getNewFileName(config.basePath(), VALUES_FILENAME_PREFIX);
        String writeOffsetsFileName = FileUtils.getNewFileName(config.basePath(), OFFSETS_FILENAME_PREFIX);
        writeValuesFilePath = config.basePath().resolve(writeValuesFileName);
        writeOffsetsFilePath = config.basePath().resolve(writeOffsetsFileName);

        if (Files.notExists(writeValuesFilePath) || Files.notExists(writeOffsetsFilePath)) {
            comparator = null;
            readArena = null;
            readValuesMSList = Collections.emptyList();
            readOffsetsMSList = Collections.emptyList();
            offsetFiles = Collections.emptyList();
            return;
        }

        comparator = new MemorySegmentComparator();
        readArena = Arena.ofShared();
        readValuesMSList = new ArrayList<>();
        readOffsetsMSList = new ArrayList<>();
        offsetFiles = new ArrayList<>();
        initReadMSLists(config);
    }

    private void initReadMSLists(Config config) throws IOException {
        File folder = new File(config.basePath().toUri());
        File[] listOfFiles = folder.listFiles();
        if (listOfFiles == null) {
            return;
        }

        for (File file : Arrays.stream(listOfFiles)
                .filter(File::isFile)
                .sorted(Comparator.comparing(File::getName))
                .toList()) {
            if (file.getName().startsWith(VALUES_FILENAME_PREFIX)) {
                readValuesMSList.add(getMappedMemory(file));
            } else {
                offsetFiles.add(file);
                readOffsetsMSList.add(getMappedMemory(file));
            }
        }

        if (readValuesMSList.size() != readOffsetsMSList.size()) {
            throw new IllegalArgumentException(
                    "Directory in config must contain same number of files with name: \"%s\" and \"%s\""
                            .formatted(VALUES_FILENAME_PREFIX, OFFSETS_FILENAME_PREFIX));
        }
    }

    private MemorySegment getMappedMemory(File file) throws IOException {
        try (FileChannel offsetsChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            return offsetsChannel.map(FileChannel.MapMode.READ_ONLY, 0, offsetsChannel.size(), readArena);
        }
    }

    Entry<MemorySegment> read(MemorySegment msKey) {
        // TODO: нужно поискать бинарным поиском по всем файлам, начиная с памяти, далее по файлам на диске (с самого старого)
        return null;
        /*if (readValuesMS == null || readOffsetsMS == null) {
            return null;
        }

        long entryOffset = Utils.binarySearch(readValuesMS, readOffsetsMS, msKey, comparator);
        if (entryOffset == -1) {
            return null;
        }

        long keySizeOffset = readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, entryOffset);
        MemorySegment key = Utils.getBySizeOffset(readValuesMS, keySizeOffset);
        long valueSizeOffset = keySizeOffset + Long.BYTES + key.byteSize();
        MemorySegment value = Utils.getBySizeOffset(readValuesMS, valueSizeOffset);

        return new BaseEntry<>(
                MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)),
                MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)));*/
    }

    public List<Iterator<Entry<MemorySegment>>> getFilesIterators(MemorySegment from,
                                                                  MemorySegment to) throws IOException {
        List<Iterator<Entry<MemorySegment>>> filesIterators = new ArrayList<>();

        for (int i = 0; i < readValuesMSList.size(); i++) {
            filesIterators.add(FileIterator.create(
                    readValuesMSList.get(i),
                    readOffsetsMSList.get(i),
                    from,
                    to,
                    comparator,
                    offsetFiles.get(i).length()));
        }

        return filesIterators;
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
                    Entry<MemorySegment> entry = it.next();
                    offsetsFileOffset = Utils.writeEntryOffset(valuesFileOffset, offsetsStorage, offsetsFileOffset);
                    valuesFileOffset = Utils.writeEntry(entry, valuesStorage, valuesFileOffset);
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
}
