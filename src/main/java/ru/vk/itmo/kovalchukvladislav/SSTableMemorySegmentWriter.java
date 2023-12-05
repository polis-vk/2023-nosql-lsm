package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Entry;
import ru.vk.itmo.kovalchukvladislav.model.EntryExtractor;
import ru.vk.itmo.kovalchukvladislav.model.TableInfo;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class SSTableMemorySegmentWriter<D, E extends Entry<D>> {
    private static final Logger logger = Logger.getLogger(SSTableMemorySegmentWriter.class.getSimpleName());
    private static final OpenOption[] WRITE_OPTIONS = new OpenOption[] {
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.CREATE
    };

    private static final StandardCopyOption[] MOVE_OPTIONS = new StandardCopyOption[] {
            StandardCopyOption.ATOMIC_MOVE,
            StandardCopyOption.REPLACE_EXISTING
    };

    private final Path basePath;
    private final String metadataFilename;
    private final String dbFilenamePrefix;
    private final String offsetsFilenamePrefix;
    private final EntryExtractor<D, E> extractor;

    public SSTableMemorySegmentWriter(Path basePath, String dbFilenamePrefix, String offsetsFilenamePrefix,
                                      String metadataFilename, EntryExtractor<D, E> extractor) {
        this.basePath = basePath;
        this.dbFilenamePrefix = dbFilenamePrefix;
        this.offsetsFilenamePrefix = offsetsFilenamePrefix;
        this.metadataFilename = metadataFilename;
        this.extractor = extractor;
        logger.setLevel(Level.OFF); // чтобы не засорять вывод в гитхабе, если такое возможно
    }

    public void compact(Iterator<E> iterator, TableInfo info) throws IOException {
        Path tempDirectory = Files.createTempDirectory(null);
        String timestamp = String.valueOf(System.currentTimeMillis());

        Path newSSTable = basePath.resolve(dbFilenamePrefix + timestamp);
        Path newOffsetsTable = basePath.resolve(offsetsFilenamePrefix + timestamp);
        Path tmpSSTable = tempDirectory.resolve(dbFilenamePrefix + timestamp);
        Path tmpOffsetsTable = tempDirectory.resolve(offsetsFilenamePrefix + timestamp);

        logger.info(() -> String.format("Compacting started to dir %s, timestamp %s, info %s",
                tempDirectory, timestamp, info));

        try {
            writeData(tempDirectory, timestamp, iterator, info);
            Path tmpMetadata = addSSTableId(tempDirectory, timestamp);
            Path newMetadata = basePath.resolve(metadataFilename);

            Files.move(tmpSSTable, newSSTable, MOVE_OPTIONS);
            Files.move(tmpOffsetsTable, newOffsetsTable, MOVE_OPTIONS);
            Files.move(tmpMetadata, newMetadata, MOVE_OPTIONS);
        } catch (Exception e) {
            deleteUnusedFiles(newSSTable, newOffsetsTable);
            throw e;
        } finally {
            deleteUnusedFiles(tempDirectory);
        }
        logger.info(() -> String.format("Compacted to dir %s, timestamp %s", basePath, timestamp));
    }

    public void flush(Iterator<E> iterator, TableInfo info) throws IOException {
        Path tempDirectory = Files.createTempDirectory(null);
        String timestamp = String.valueOf(System.currentTimeMillis());

        Path newSSTable = basePath.resolve(dbFilenamePrefix + timestamp);
        Path newOffsetsTable = basePath.resolve(offsetsFilenamePrefix + timestamp);
        Path tmpSSTable = tempDirectory.resolve(dbFilenamePrefix + timestamp);
        Path tmpOffsetsTable = tempDirectory.resolve(offsetsFilenamePrefix + timestamp);

        logger.info(() -> String.format("Flushing started to dir %s, timestamp %s, info %s",
                tempDirectory, timestamp, info));
        try {
            writeData(tempDirectory, timestamp, iterator, info);

            Files.move(tmpSSTable, newSSTable, MOVE_OPTIONS);
            Files.move(tmpOffsetsTable, newOffsetsTable, MOVE_OPTIONS);
            addSSTableId(basePath, timestamp);
        } catch (Exception e) {
            deleteUnusedFiles(newSSTable, newOffsetsTable);
            throw e;
        } finally {
            deleteUnusedFilesInDirectory(tempDirectory);
        }
        logger.info(() -> String.format("Flushed to dir %s, timestamp %s", basePath, timestamp));
    }

    // Удаление ненужных файлов не является чем то критически важным
    // Если произойдет исключение, лучше словить и вывести в лог, чем останавливать работу
    public void deleteUnusedFiles(Path... files) {
        for (Path file : files) {
            try {
                boolean deleted = Files.deleteIfExists(file);
                if (deleted) {
                    logger.info(() -> String.format("File %s was deleted", file));
                } else {
                    logger.severe(() -> String.format("File %s not deleted", file));
                }
            } catch (IOException e) {
                logger.severe(() -> String.format("Error while deleting file %s: %s", file, e.getMessage()));
            }
        }
    }

    private void deleteUnusedFilesInDirectory(Path directory) {
        try (Stream<Path> files = Files.walk(directory)) {
            Path[] array = files.sorted(Comparator.reverseOrder()).toArray(Path[]::new);
            deleteUnusedFiles(array);
        } catch (Exception e) {
            logger.severe(() -> String.format("Error while deleting directory %s: %s", directory, e.getMessage()));
        }
    }

    private void writeData(Path path, String timestamp, Iterator<E> daoIterator, TableInfo info) throws IOException {
        Path dbPath = path.resolve(dbFilenamePrefix + timestamp);
        Path offsetsPath = path.resolve(offsetsFilenamePrefix + timestamp);

        try (FileChannel db = FileChannel.open(dbPath, WRITE_OPTIONS);
             FileChannel offsets = FileChannel.open(offsetsPath, WRITE_OPTIONS);
             Arena arena = Arena.ofConfined()) {

            long offsetsSize = info.getRecordsCount() * Long.BYTES;
            MemorySegment fileSegment = db.map(FileChannel.MapMode.READ_WRITE, 0, info.getRecordsSize(), arena);
            MemorySegment offsetsSegment = offsets.map(FileChannel.MapMode.READ_WRITE, 0, offsetsSize, arena);

            int i = 0;
            long offset = 0;
            while (daoIterator.hasNext()) {
                E entry = daoIterator.next();
                offsetsSegment.setAtIndex(ValueLayout.JAVA_LONG_UNALIGNED, i, offset);
                offset = extractor.writeEntry(entry, fileSegment, offset);
                i += 1;
            }

            fileSegment.load();
            offsetsSegment.load();
        }
    }

    private Path addSSTableId(Path path, String id) throws IOException {
        return Files.writeString(path.resolve(metadataFilename), id + System.lineSeparator(),
                StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE);
    }
}
