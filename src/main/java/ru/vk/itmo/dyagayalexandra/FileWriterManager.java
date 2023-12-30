package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Iterator;

public class FileWriterManager {

    private final String fileName;
    private final String fileExtension;
    private final String fileIndexName;
    private final Path basePath;
    private final Arena arena;

    public FileWriterManager(String fileName, String fileExtension, String fileIndexName, Path basePath, Arena arena) {
        this.fileName = fileName;
        this.fileExtension = fileExtension;
        this.fileIndexName = fileIndexName;
        this.basePath = basePath;
        this.arena = arena;
    }

    SavedFilesInfo save(Collection<Entry<MemorySegment>> entryCollection,
                       int tablesSize, int indexesSize) throws IOException {
        Path tablePath = basePath.resolve(fileName + tablesSize + fileExtension);
        Path indexPath = basePath.resolve(fileIndexName + indexesSize + fileExtension);

        long tableSize = 0;

        Iterator<Entry<MemorySegment>> storageIterator = entryCollection.iterator();
        long storageSize = 0;

        while (storageIterator.hasNext()) {
            Entry<MemorySegment> entry = storageIterator.next();
            tableSize += 2 * Integer.BYTES + entry.key().byteSize();
            if (entry.value() != null) {
                tableSize += entry.value().byteSize();
            }
            storageSize++;
        }

        MemorySegment tableMemorySegment;
        MemorySegment indexMemorySegment;

        try (FileChannel tableChannel = FileChannel.open(tablePath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            tableMemorySegment = tableChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, tableSize, arena);
        }

        try (FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            indexMemorySegment = indexChannel.map(FileChannel.MapMode.READ_WRITE,
                    0, (storageSize + 1) * Long.BYTES, arena);
        }

        long indexOffset = Long.BYTES;
        long offset;
        long tableOffset = 0;

        storageIterator = entryCollection.iterator();

        writeStorageSize(indexMemorySegment, storageSize);
        for (int i = 0; i < storageSize; i++) {
            Entry<MemorySegment> entry = storageIterator.next();
            offset = writeEntry(tableMemorySegment, tableOffset, entry);
            writeIndexes(indexMemorySegment, indexOffset, tableOffset);
            tableOffset = offset;
            indexOffset += Long.BYTES;
        }

        return new SavedFilesInfo(tableMemorySegment, indexMemorySegment, tablePath, indexPath);
    }

    long writeEntry(MemorySegment tableMemorySegment, long offset, Entry<MemorySegment> entry) {
        long tableFileOffset = offset;

        int keyLength = (int) entry.key().byteSize();

        tableMemorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, tableFileOffset, keyLength);
        tableFileOffset += Integer.BYTES;

        MemorySegment.copy(entry.key(), 0, tableMemorySegment, tableFileOffset, keyLength);
        tableFileOffset += keyLength;

        if (entry.value() == null) {
            tableMemorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, tableFileOffset, -1);
            tableFileOffset += Integer.BYTES;
        } else {
            int valueLength = (int) entry.value().byteSize();

            tableMemorySegment.set(ValueLayout.JAVA_INT_UNALIGNED, tableFileOffset, valueLength);
            tableFileOffset += Integer.BYTES;

            MemorySegment.copy(entry.value(), 0, tableMemorySegment, tableFileOffset, valueLength);
            tableFileOffset += valueLength;
        }

        return tableFileOffset;
    }

    void writeIndexes(MemorySegment indexMemorySegment, long indexOffset, long offset) {
        indexMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, offset);
    }

    void writeStorageSize(MemorySegment indexMemorySegment, long storageSize) {
        indexMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, storageSize);
    }

    public static class SavedFilesInfo {

        private final MemorySegment ssTable;
        private final MemorySegment ssIndex;
        private final Path ssTablePath;
        private final Path ssIndexPath;

        public SavedFilesInfo(MemorySegment ssTable, MemorySegment ssIndex, Path ssTablePath, Path ssIndexPath) {
            this.ssTable = ssTable;
            this.ssIndex = ssIndex;
            this.ssTablePath = ssTablePath;
            this.ssIndexPath = ssIndexPath;
        }

        public MemorySegment getSSTable() {
            return ssTable;
        }

        public MemorySegment getSSIndex() {
            return ssIndex;
        }

        public Path getSSTablePath() {
            return ssTablePath;
        }

        public Path getSSIndexPath() {
            return ssIndexPath;
        }
    }
}
