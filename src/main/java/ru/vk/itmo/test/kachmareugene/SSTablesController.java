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
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class SSTablesController {
    private final Path ssTablesDir;
    private final List<MemorySegment> ssTables = new ArrayList<>();
    private final List<MemorySegment> ssTablesIndexes = new ArrayList<>();
    private final List<Path> ssTablesPaths = new ArrayList<>();
    private final List<Path> ssTablesIndexesPaths = new ArrayList<>();
    private static final String SS_TABLE_COMMON_PREF = "ssTable";
    //  index format: (long) keyOffset, (long) keyLen, (long) valueOffset, (long) valueLen
    private static final long ONE_LINE_SIZE = 4 * Long.BYTES;
    private static final String INDEX_COMMON_PREF = "index";
    private final Arena arenaForReading = Arena.ofShared();
    private boolean isClosedArena;
    private final Comparator<MemorySegment> segComp;

    public SSTablesController(Path dir, Comparator<MemorySegment> com) {
        this.ssTablesDir = dir;
        this.segComp = com;

        ssTablesPaths.addAll(openFiles(dir, SS_TABLE_COMMON_PREF, ssTables));
        ssTablesIndexesPaths.addAll(openFiles(dir, INDEX_COMMON_PREF, ssTablesIndexes));
    }

    public SSTablesController(Comparator<MemorySegment> com) {
        this.ssTablesDir = null;
        this.segComp = com;
    }

    private List<Path> openFiles(Path dir, String fileNamePref, List<MemorySegment> storage) {
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Stream<Path> tabels = Files.find(dir, 1,
                (path, ignore) -> path.getFileName().toString().startsWith(fileNamePref))) {
            final List<Path> list = new ArrayList<>(tabels.toList());
            Utils.sortByNames(list, fileNamePref);
            list.forEach(t -> {
                try (FileChannel channel = FileChannel.open(t, READ)) {
                    storage.add(channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arenaForReading));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            return list;
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
            long entryIndexesLine = 0;
            if (key != null) {
                entryIndexesLine = searchKeyInFile(ssTablesIndexes.get(i), ssTables.get(i), key);
             }
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

    public void dumpIterator(Iterable<Entry<MemorySegment>> iter) throws IOException {
        Iterator<Entry<MemorySegment>> iter1 = iter.iterator();

        if (ssTablesDir == null || !iter1.hasNext()) {
            closeArena();
            return;
        }
        String suff = String.valueOf(Utils.getMaxNumberOfFile(ssTablesDir, SS_TABLE_COMMON_PREF) + 1);
        Set<OpenOption> options = Set.of(WRITE, READ, CREATE);
        try (FileChannel ssTableChannel =
                     FileChannel.open(ssTablesDir
                             .resolve(SS_TABLE_COMMON_PREF + suff), options);
             FileChannel indexChannel =
                     FileChannel.open(ssTablesDir
                             .resolve(INDEX_COMMON_PREF + suff), options);
             Arena saveArena = Arena.ofConfined()) {

            long ssTableLenght = 0L;
            long indexLength = 0L;
            while (iter1.hasNext()) {
                var seg = iter1.next();
                ssTableLenght += seg.key().byteSize() + getValueOrNull(seg).byteSize();
                indexLength += ONE_LINE_SIZE;
            }
            long currOffsetSSTable = 0L;
            long currOffsetIndex = 0L;

            MemorySegment mappedSSTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetSSTable, ssTableLenght, saveArena);
            MemorySegment mappedIndex = indexChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetIndex, indexLength, saveArena);

            for (Entry<MemorySegment> kv : iter) {
                currOffsetIndex = Utils.dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = Utils.dumpSegment(mappedSSTable, kv.key(), currOffsetSSTable);
                currOffsetIndex = Utils.dumpLong(mappedIndex, kv.key().byteSize(), currOffsetIndex);

                currOffsetIndex = Utils.dumpLong(mappedIndex, currOffsetSSTable, currOffsetIndex);
                currOffsetSSTable = Utils.dumpSegment(mappedSSTable, getValueOrNull(kv), currOffsetSSTable);
                currOffsetIndex = Utils.dumpLong(mappedIndex, rightByteSize(kv), currOffsetIndex);
            }
        } finally {
            closeArena();
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
    private void closeArena() {
        if (!isClosedArena) {
            arenaForReading.close();
        }
        isClosedArena = true;
    }

    public void deleteAllOldFiles() throws IOException {
        closeArena();
        deleteFiles(ssTablesPaths);
        deleteFiles(ssTablesIndexesPaths);
    }

    private void deleteFiles(List<Path> files) throws IOException {
        for (Path file : files) {
            Files.deleteIfExists(file);
        }
        files.clear();
    }
}
