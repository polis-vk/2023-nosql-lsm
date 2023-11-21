package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.SortedMap;
import java.util.stream.Stream;

public class SSTablesController {
    private final Path ssTablesDir;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss.SSSSS");
    private final List<MemorySegment> ssTables = new ArrayList<>();
    private final List<MemorySegment> ssTablesIndexes = new ArrayList<>();
    private static final String SS_TABLE_COMMON_PREF = "ssTable";

    // index format: (long) keyOffset, (long) keyLen, (long) valueOffset, (long) valueLen
    private static final long ONE_LINE_SIZE = 4 * Long.BYTES;
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
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Stream<Path> tabels = Files.find(dir, 1,
                (path, ignore) -> path.getFileName().toString().startsWith(fileNamePref))) {
            final List<Path> list = new ArrayList<>(tabels.toList());
            Collections.sort(list);
            list.forEach(t -> {
                try (FileChannel channel = FileChannel.open(t, StandardOpenOption.READ)) {
                    storage.add(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arenaForReading));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private boolean greaterThen(MemorySegment mappedIndex, long lineInBytesOffset,
                                MemorySegment mappedData, MemorySegment key) {
        long offset = mappedIndex.get(ValueLayout.JAVA_LONG, lineInBytesOffset);
        long size = mappedIndex.get(ValueLayout.JAVA_LONG, lineInBytesOffset + Long.BYTES);
        return segComp.compare(key, mappedData.asSlice(offset, size)) > 0;
    }

    //Gives offset for line in index file
    private long searchKeyInFile(MemorySegment mappedIndex, MemorySegment mappedData, MemorySegment key) {
        long l = -1;
        long r = mappedIndex.byteSize() / ONE_LINE_SIZE;

        while (r - l > 1) {
            long mid = (l + r) / 2;

            if (greaterThen(mappedIndex, mid * ONE_LINE_SIZE, mappedData, key)) {
                l = mid;
            } else {
                r = mid;
            }
        }
        return r == (mappedIndex.byteSize() / ONE_LINE_SIZE) ? -1 : r;
    }

    //return - List ordered form the latest created sstable to the first.
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

    private SSTableRowInfo createRowInfo(int ind, long rowShift) {
        long start = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG, rowShift * ONE_LINE_SIZE);
        long size = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG, rowShift * ONE_LINE_SIZE + Long.BYTES);

        long start1 = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG,rowShift * ONE_LINE_SIZE + Long.BYTES * 2);
        long size1 = ssTablesIndexes.get(ind).get(ValueLayout.JAVA_LONG,rowShift * ONE_LINE_SIZE + Long.BYTES * 3);
        return new SSTableRowInfo(start, size, start1, size1, ind, rowShift);
    }

    public SSTableRowInfo searchInSStables(MemorySegment key) {
        for (int i = ssTablesIndexes.size() - 1; i >= 0; i--) {
            long ind = searchKeyInFile(ssTablesIndexes.get(i), ssTables.get(i), key);
            if (ind >= 0) {
                return createRowInfo(i, ind);
            }
        }
        return null;
    }

    public Entry<MemorySegment> getRow(SSTableRowInfo info) {
        if (info == null) {
            return null;
        }

        var key = ssTables.get(info.ssTableInd).asSlice(info.keyOffset, info.keySize);

        if (info.isDeletedData()) {
            return new BaseEntry<>(key, null);
        }
        var value = ssTables.get(info.ssTableInd).asSlice(info.valueOffset, info.getValueSize());

        return new BaseEntry<>(key, value);
    }

    /**
     * Ignores deleted values.
     */
    public SSTableRowInfo getNextInfo(SSTableRowInfo info, MemorySegment maxKey) {
        for (long t = info.rowShift + 1; t < ssTablesIndexes.get(info.ssTableInd).byteSize() / ONE_LINE_SIZE; t++) {
            var inf = createRowInfo(info.ssTableInd, t);

            Entry<MemorySegment> row = getRow(inf);
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

        if (ssTablesDir == null || mp.isEmpty()) {
            arenaForReading.close();
            return;
        }
        LocalDateTime time = LocalDateTime.now(ZoneId.systemDefault());
        try (FileChannel ssTableChannel =
                     FileChannel.open(ssTablesDir.resolve(SS_TABLE_COMMON_PREF + formatter.format(time)),
                             StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
             FileChannel indexChannel =
                     FileChannel.open(ssTablesDir.resolve(INDEX_COMMON_PREF + formatter.format(time)),
                             StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE);
             Arena saveArena = Arena.ofConfined()) {

            long ssTableLenght = 0L;
            long indexLength = mp.size() * ONE_LINE_SIZE;

            for (var kv : mp.values()) {
                ssTableLenght +=
                        kv.key().byteSize() + getValueOrNull(kv).byteSize();
            }

            long currOffsetSSTable = 0L;
            long currOffsetIndex = 0L;

            MemorySegment mappedSSTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetSSTable, ssTableLenght, saveArena);

            MemorySegment mappedIndex = indexChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetIndex, indexLength, saveArena);

            for (var kv : mp.values()) {
                currOffsetIndex = dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = dumpSegment(mappedSSTable, kv.key(), currOffsetSSTable);
                currOffsetIndex = dumpLong(mappedIndex, kv.key().byteSize(), currOffsetIndex);

                currOffsetIndex = dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = dumpSegment(mappedSSTable, getValueOrNull(kv), currOffsetSSTable);
                currOffsetIndex = dumpLong(mappedIndex, rightByteSize(kv), currOffsetIndex);
            }
        } finally {
            arenaForReading.close();
        }
    }

    private long rightByteSize(Entry<MemorySegment> memSeg) {
        if (memSeg.value() == null) {
            return -1;
        }
        return memSeg.value().byteSize();
    }

    private MemorySegment getValueOrNull(Entry<MemorySegment> kv) {
        MemorySegment value = kv.value();
        if (kv.value() == null) {
            value = MemorySegment.NULL;
        }
        return value;
    }
}
