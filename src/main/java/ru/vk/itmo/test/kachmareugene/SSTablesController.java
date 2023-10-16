package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.*;

public class SSTablesController {

    private final Path ssTablesDir;

    private final List<MemorySegment> ssTables = new ArrayList<>();
    private final List<MemorySegment> ssTablesIndexes = new ArrayList<>();
    private static final String SS_TABLE_COMMON_PREF = "ssTable";

    // index format: (long) keyOffset, (long) keyLen, (long) valueOffset, (long) valueLen
    private static final long oneLineSize = 4 * Long.BYTES;
    private static final String INDEX_COMMON_PREF = "index";
    private final Arena arenaForReading = Arena.ofShared();
    private final Comparator<MemorySegment> segComp;

    public SSTablesController(Path dir, Comparator<MemorySegment> com) {
        this.ssTablesDir = dir;
        this.segComp = com;


        openFiles(dir, SS_TABLE_COMMON_PREF, ssTables);
        openFiles(dir, INDEX_COMMON_PREF, ssTablesIndexes);
    }
    public SSTablesController(Comparator<MemorySegment> com) {
        this.ssTablesDir = null;
        this.segComp = com;
    }

    private void openFiles(Path dir, String fileNamePref, List<MemorySegment> storage) {
        try (Stream<Path> tabels = Files.find(dir, 1,
                (path, ignore) -> path.getFileName().toString().startsWith(fileNamePref))) {
            tabels.forEach(t -> {
                try (FileChannel channel = FileChannel.open(t, READ)) {
                    storage.add(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arenaForReading));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(String.format("Cannot open %s %s", fileNamePref, e.getMessage()));
        }
    }

    private boolean lessThenKey(MemorySegment mappedIndex, long lineInBytesOffset, MemorySegment mappedData, MemorySegment key) {
        long offset = mappedIndex.get(ValueLayout.JAVA_LONG, lineInBytesOffset);
        long size = mappedIndex.get(ValueLayout.JAVA_LONG, lineInBytesOffset + Long.BYTES);

        return segComp.compare(mappedData.asSlice(offset, size), key) < 0;
    }

    /**
     Gives offset for line in index file
     **/
    private long searchKeyInFile(MemorySegment mappedIndex,  MemorySegment mappedData, MemorySegment key) {
        long l = -1;
        long r = mappedIndex.byteSize() / oneLineSize;

        while (r - l > 1) {
            long mid = (l + r) / 2;

            if (lessThenKey(mappedIndex, mid * oneLineSize, mappedData, key) ) {
                l = mid;
            } else {
                r = mid;
            }
        }
        return r == (mappedIndex.byteSize() / oneLineSize) ? -1 : r;
    }

    /**
     * -
     * @return List ordered form the latest created sstable to the first.
     */
    public List<SSTableRowInfo> firstGreaterKeys(MemorySegment key) {
        List<SSTableRowInfo> ans = new ArrayList<>();

        for (int i = ssTables.size() - 1; i >= 0; i--) {
            long entryIndexesLine = searchKeyInFile(ssTablesIndexes.get(i), ssTables.get(i), key);
            if (entryIndexesLine < 0) {
                continue;
            }
            ans.add(createRowInfo(i, entryIndexesLine));
        }
        return ans;
    }
    // fixme for loop and smart set
    private SSTableRowInfo createRowInfo(int ind, long offset) {
        long start = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG, offset * oneLineSize);
        long size = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG, offset * oneLineSize + Long.BYTES);

        long start1 = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG,offset * oneLineSize + Long.BYTES * 2);
        long size1 = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG,offset * oneLineSize + Long.BYTES * 3);
        return new SSTableRowInfo(start, size, start1, size1, ind);
    }

    public SSTableRowInfo searchInSStables(MemorySegment key) {
        for (int i = ssTablesIndexes.size() - 1; i >= 0; i--) {
            long ind = searchKeyInFile(ssTablesIndexes.get(i), ssTables.get(i), key);
            if (ind < 0) {
                continue;
            }

            return createRowInfo(i, ind);
        }
        return null;
    }

    public Entry<MemorySegment> getRow(SSTableRowInfo info) {
        if (info == null || info.valueSize == 0) {
            return null;
        }

        var key = ssTables.get(info.SSTableInd).asSlice(info.keyOffset, info.keySize);
        var value = ssTables.get(info.SSTableInd).asSlice(info.valueOffset, info.valueSize);

        return new BaseEntry<>(key, value);
    }

    /**
     * Ignores deleted values.
     */
    public SSTableRowInfo getNextInfo(SSTableRowInfo info, MemorySegment maxKey) {

        for (long t = info.keyOffset / oneLineSize + 1; t < ssTablesIndexes.size() / oneLineSize; t++) {
            var inf = createRowInfo(info.SSTableInd, t);
            Entry<MemorySegment> row = getRow(inf);

            if (row.value().byteSize() == 0) {
                continue;
            }

            if (segComp.compare(row.key(), maxKey) < 0) {
                return inf;
            }
        }


        return null;
    }

    private long dumpLong(MemorySegment mapped, long value, long offset) {
        mapped.set(ValueLayout.JAVA_LONG, offset, value);
        return offset + Long.BYTES;
    }

    private long dumpSegment(MemorySegment mapped, MemorySegment data, long offset) {
        MemorySegment.copy(data, 0, mapped, offset, data.byteSize());
        return offset + data.byteSize();
    }


    public void dumpMemTableToSStable(SortedMap<MemorySegment, Entry<MemorySegment>> mp) throws IOException {

        // fixme as nullpointer exeption
        if (ssTablesDir == null) {
            arenaForReading.close();
            return;
        }

        Set<OpenOption> options = Set.of(WRITE, READ, CREATE);

        try (FileChannel ssTableChannel =
                     FileChannel.open(ssTablesDir.resolve(SS_TABLE_COMMON_PREF + ssTables.size()), options);
             FileChannel indexChannel =
                     FileChannel.open(ssTablesDir.resolve(INDEX_COMMON_PREF + ssTables.size()), options)) {

            long ssTableLenght = 0L;
            long indexLength = mp.size() * oneLineSize;

            for (var kv : mp.values()) {
                ssTableLenght +=
                        kv.key().byteSize() + getValueOrNull(kv).byteSize();
            }

            long currOffsetSSTable = 0L;
            long currOffsetIndex = 0L;

            MemorySegment mappedSSTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetSSTable, ssTableLenght, Arena.ofConfined());

            MemorySegment mappedIndex = indexChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetIndex, indexLength, Arena.ofConfined());

            for (var kv : mp.values()) {
                currOffsetIndex = dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = dumpSegment(mappedSSTable, kv.key(), currOffsetSSTable);
                currOffsetIndex = dumpLong(mappedIndex, kv.key().byteSize(), currOffsetIndex);

                currOffsetIndex = dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = dumpSegment(mappedSSTable, getValueOrNull(kv), currOffsetSSTable);
                currOffsetIndex = dumpLong(mappedIndex, getValueOrNull(kv).byteSize(), currOffsetIndex);
            }
        } finally {
            arenaForReading.close();
        }
    }
    private MemorySegment getValueOrNull(Entry<MemorySegment> kv) {
        MemorySegment value = kv.value();
        if (kv.value() == null) {
            value = MemorySegment.NULL;
        }
        return value;
    }
}
