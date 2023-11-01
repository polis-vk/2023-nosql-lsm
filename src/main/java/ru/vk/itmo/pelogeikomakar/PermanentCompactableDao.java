package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class PermanentCompactableDao extends PermanentDao {

    private boolean compactedOrClosed;
    private static final String ZERO_TMP = "0.tmp";
    private static final String COMPACT_STAT = "compact.stat";

    public PermanentCompactableDao(Config config) throws IOException {
        if (config == null) {
            daoConfig = null;
            return;
        }
        daoConfig = config;

        checkAndRecover();
        loadTables();

    }

    private void checkAndRecover() throws IOException {
        var tempTablePath = daoConfig.basePath()
                .resolve(SSTABLE_NAME + ZERO_TMP);
        var statFilePath = daoConfig.basePath()
                .resolve(COMPACT_STAT);
        MemorySegment tempTable;
        var tempIndexPath = daoConfig.basePath()
                .resolve(INDEX_NAME + ZERO_TMP);
        Files.deleteIfExists(tempIndexPath);

        if (Files.exists(tempTablePath)) {
            try (FileChannel tableFile = FileChannel.open(tempTablePath, StandardOpenOption.READ)) {
                tempTable = tableFile.map(FileChannel.MapMode.READ_ONLY, 0,
                        Files.size(tempTablePath), arenaTableFiles);
            }

            if (Files.exists(statFilePath)) {
                loadTmpTable(tempTable);
            } else {
                makeCompactDone(tempTable, tempIndexPath, statFilePath);
            }
        }

        Files.deleteIfExists(statFilePath);
        Files.deleteIfExists(tempTablePath);
        Files.deleteIfExists(tempIndexPath);

    }

    private void makeCompactDone(MemorySegment tempTable, Path tempIndexPath, Path statFilePath) throws IOException {
        List<Long> dataOffsets = new ArrayList<>();
        long offset = 0;
        while (offset < tempTable.byteSize()) {
            dataOffsets.add(offset);
            long sizeOfKey = tempTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            long valueOffset = offset + Long.BYTES + sizeOfKey;
            long valueSize = tempTable.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);

            offset = valueOffset + Long.BYTES + valueSize;
        }
        long indexTableSize = (long) dataOffsets.size() * Long.BYTES;

        try (var arenaWriter = Arena.ofShared(); var fileIndexOut = FileChannel.open(tempIndexPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {

            MemorySegment memSegmentIndexOut = fileIndexOut.map(FileChannel.MapMode.READ_WRITE,
                    0, indexTableSize, arenaWriter);

            long offsetIndex = 0;
            for (var dataPtr : dataOffsets) {
                memSegmentIndexOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, dataPtr);
                offsetIndex += Long.BYTES;
            }

        }

        int oldTablesNumber;
        try (var dis = new DataInputStream(new FileInputStream(statFilePath.toString()))) {
            oldTablesNumber = dis.readInt();
        }

        deleteOldFiles(false, oldTablesNumber);

        Path sourceTable = daoConfig.basePath()
                .resolve(SSTABLE_NAME + ZERO_TMP);
        Path targetTable = daoConfig.basePath()
                .resolve(SSTABLE_NAME + Integer.toString(0));

        Path sourceIndex = daoConfig.basePath()
                .resolve(INDEX_NAME + ZERO_TMP);
        Path targetIndex = daoConfig.basePath()
                .resolve(INDEX_NAME + Integer.toString(0));

        renameFile(sourceTable, targetTable);
        renameFile(sourceIndex, targetIndex);
    }

    private void loadTmpTable(MemorySegment tempTable) {
        long offset = 0;
        while (offset < tempTable.byteSize()) {
            long sizeOfKey = tempTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            MemorySegment currKey = tempTable.asSlice(offset + Long.BYTES, sizeOfKey);

            long valueOffset = offset + Long.BYTES + sizeOfKey;
            long valueSize = tempTable.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);

            MemorySegment currValue = tempTable.asSlice(valueOffset + Long.BYTES, valueSize);
            var entry = new BaseEntry<MemorySegment>(currKey, currValue);
            mapCurrent.put(currKey, entry);

            offset = valueOffset + Long.BYTES + valueSize;
        }
    }

    private void loadActualData() {
        for (int ssTableNum = maxSSTable; ssTableNum >= 0; --ssTableNum) {
            MemorySegment tableCurr = ssTableMap.get(ssTableNum);
            long tableSize = tableCurr.byteSize();
            long dataOffset = 0;

            while (dataOffset < tableSize) {
                long sizeOfKey = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset);
                MemorySegment currKey = tableCurr.asSlice(dataOffset + Long.BYTES, sizeOfKey);

                long valueOffset = dataOffset + Long.BYTES + sizeOfKey;
                long valueSize = tableCurr.get(ValueLayout.JAVA_LONG_UNALIGNED, valueOffset);

                if (! mapCurrent.containsKey(currKey) && valueSize > 0) {
                    MemorySegment currValue = tableCurr.asSlice(valueOffset + Long.BYTES, valueSize);
                    var entry = new BaseEntry<MemorySegment>(currKey, currValue);
                    mapCurrent.put(currKey, entry);
                }

                dataOffset = valueOffset + Long.BYTES + (valueSize > 0 ? valueSize : 0);
            }
        }
    }

    private void deleteOldFiles(boolean knownSize, int size) throws IOException {
        if (knownSize) {
            arenaTableFiles.close();
            for (int ssTableNum = size; ssTableNum >= 0; --ssTableNum) {
                Files.deleteIfExists(daoConfig.basePath()
                        .resolve(SSTABLE_NAME + Integer.toString(ssTableNum)));
                Files.deleteIfExists(daoConfig.basePath()
                        .resolve(INDEX_NAME + Integer.toString(ssTableNum)));
            }
            maxSSTable = 0;
        } else {
            for (int ssTableNum = size; ssTableNum >= 0; --ssTableNum) {
                Files.deleteIfExists(daoConfig.basePath()
                        .resolve(SSTABLE_NAME + Integer.toString(ssTableNum)));
                Files.deleteIfExists(daoConfig.basePath()
                        .resolve(INDEX_NAME + Integer.toString(ssTableNum)));
            }
        }
    }

    private long[] countOutFilesSize() {
        long ssTableSizeOut = 0;
        long indexTableSize = 0;
        for (var item : mapCurrent.values()) {
            if (item.value() == null) {
                continue;
            }
            indexTableSize += 1;
            ssTableSizeOut += item.key().byteSize() + item.value().byteSize() + Long.BYTES * 2L;
        }
        indexTableSize *= Long.BYTES;

        return new long[]{indexTableSize, ssTableSizeOut};
    }

    private void writeTmpTable() throws IOException {
        if (!mapCurrent.isEmpty()) {
            var sizes = countOutFilesSize();
            long indexTableSize = sizes[0];
            long ssTableSizeOut = sizes[1];

            Arena arenaWriter = Arena.ofShared();
            FileChannel fileDataOut = null;
            FileChannel fileIndexOut = null;

            try {
                fileDataOut = FileChannel.open(daoConfig.basePath()
                                .resolve(SSTABLE_NAME + ZERO_TMP),
                        StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                MemorySegment memSegmentDataOut = fileDataOut.map(FileChannel.MapMode.READ_WRITE,
                        0, ssTableSizeOut, arenaWriter);

                fileIndexOut = FileChannel.open(daoConfig.basePath()
                                .resolve(INDEX_NAME + ZERO_TMP),
                        StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
                MemorySegment memSegmentIndexOut = fileIndexOut.map(FileChannel.MapMode.READ_WRITE,
                        0, indexTableSize, arenaWriter);

                long offsetData = 0;
                long offsetIndex = 0;
                for (var item : mapCurrent.values()) {
                    if (item.value() == null) {
                        continue;
                    } else {
                        memSegmentIndexOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetIndex, offsetData);
                        offsetIndex += Long.BYTES;

                        memSegmentDataOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, item.key().byteSize());
                        offsetData += Long.BYTES;

                        MemorySegment.copy(item.key(), 0, memSegmentDataOut, offsetData, item.key().byteSize());
                        offsetData += item.key().byteSize();

                        memSegmentDataOut.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetData, item.value().byteSize());
                        offsetData += Long.BYTES;

                        MemorySegment.copy(item.value(), 0, memSegmentDataOut, offsetData, item.value().byteSize());
                        offsetData += item.value().byteSize();
                    }
                }
            } finally {
                arenaWriter.close();
                if (fileDataOut != null) {
                    fileDataOut.close();
                }
                if (fileIndexOut != null) {
                    fileIndexOut.close();
                }
            }
        }
    }

    private void renameFile(Path source, Path target) throws IOException {
        File fileTempTable = new File(source.toString());
        File fileOutTable = new File(target.toString());
        fileTempTable.renameTo(fileOutTable);
        Files.deleteIfExists(source);
    }

    @Override
    public void compact() throws IOException {

        compactedOrClosed = true;

        loadActualData();

        var statFilePath = daoConfig.basePath()
                .resolve(COMPACT_STAT);
        try (var dos = new DataOutputStream(new FileOutputStream(statFilePath.toString()))) {
            dos.writeInt(maxSSTable);
        }

        writeTmpTable();

        Files.deleteIfExists(statFilePath);

        deleteOldFiles(true, maxSSTable);

        if (!mapCurrent.isEmpty()) {
            Path sourceTable = daoConfig.basePath()
                    .resolve(SSTABLE_NAME + ZERO_TMP);
            Path targetTable = daoConfig.basePath()
                    .resolve(SSTABLE_NAME + Integer.toString(0));

            Path sourceIndex = daoConfig.basePath()
                    .resolve(INDEX_NAME + ZERO_TMP);
            Path targetIndex = daoConfig.basePath()
                    .resolve(INDEX_NAME + Integer.toString(0));

            renameFile(sourceTable, targetTable);
            renameFile(sourceIndex, targetIndex);
        }

    }

    @Override
    public void close() throws IOException {
        if (compactedOrClosed || mapCurrent.isEmpty()) {
            return;
        }
        compactedOrClosed = true;
        super.close();
    }
}
