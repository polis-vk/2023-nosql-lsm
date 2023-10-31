package ru.vk.itmo.pelogeikomakar;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class PermanentCompactableDao extends PermanentDao {

    private boolean compactedOrClosed;
    private static final String ZERO_TMP = "0.tmp";

    public PermanentCompactableDao(Config config) {
        super(config);
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

    private void deleteAllOldFiles() throws IOException {
        for (int ssTableNum = maxSSTable; ssTableNum >= 0; --ssTableNum) {
            arenaTableMap.get(ssTableNum).close();
            arenaTableMap.remove(ssTableNum);
            arenaIndexMap.get(ssTableNum).close();
            arenaIndexMap.remove(ssTableNum);
            Files.deleteIfExists(daoConfig.basePath()
                    .resolve(SSTABLE_NAME + Integer.toString(ssTableNum)));
            Files.deleteIfExists(daoConfig.basePath()
                    .resolve(INDEX_NAME + Integer.toString(ssTableNum)));
        }
        arenaTableMap.clear();
        arenaIndexMap.clear();
        maxSSTable = 0;
    }

    private void writeTmpTable() throws IOException {
        if (!mapCurrent.isEmpty()) {
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

            FileChannel fileDataOut = FileChannel.open(daoConfig.basePath()
                            .resolve(SSTABLE_NAME + ZERO_TMP),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            Arena arenaDataWriter = Arena.ofShared();
            MemorySegment memSegmentDataOut = fileDataOut.map(FileChannel.MapMode.READ_WRITE,
                    0, ssTableSizeOut, arenaDataWriter);

            FileChannel fileIndexOut = FileChannel.open(daoConfig.basePath()
                            .resolve(INDEX_NAME + ZERO_TMP),
                    StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            Arena arenaIndexWriter = Arena.ofShared();
            MemorySegment memSegmentIndexOut = fileIndexOut.map(FileChannel.MapMode.READ_WRITE,
                    0, indexTableSize, arenaIndexWriter);

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
            arenaDataWriter.close();
            arenaIndexWriter.close();
            fileDataOut.close();
            fileIndexOut.close();
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

        writeTmpTable();

        deleteAllOldFiles();

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
