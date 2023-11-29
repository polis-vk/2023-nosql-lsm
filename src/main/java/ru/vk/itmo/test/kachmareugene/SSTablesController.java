package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static ru.vk.itmo.test.kachmareugene.Utils.getValueOrNull;

public class SSTablesController {
    private final Path ssTablesDir;
    private final List<MemorySegment> ssTables = new ArrayList<>();
    private final List<Path> ssTablesPaths = new ArrayList<>();
    private static final String SS_TABLE_COMMON_PREF = "ssTable";
    //  index format: (long) keyOffset, (long) keyLen, (long) valueOffset, (long) valueLen
    private static final long ONE_LINE_SIZE = 4 * Long.BYTES;
    private static final Set<OpenOption> options = Set.of(StandardOpenOption.WRITE, StandardOpenOption.READ,
            StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    private final Arena arenaForReading = Arena.ofShared();
    private boolean isClosedArena;
    private final Comparator<MemorySegment> segComp;

    public SSTablesController(Path dir, Comparator<MemorySegment> com) {
        this.ssTablesDir = dir;
        this.segComp = com;

        ssTablesPaths.addAll(openFiles(dir, SS_TABLE_COMMON_PREF, ssTables));
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
                try (FileChannel channel = FileChannel.open(t, StandardOpenOption.READ)) {
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

    private boolean greaterThen(long keyOffset, long keySize,
                                MemorySegment mapped, MemorySegment key) {

        return segComp.compare(key, mapped.asSlice(keyOffset, keySize)) > 0;
    }

    //Gives offset for line in index file
    private long searchKeyInFile(int ind, MemorySegment mapped, MemorySegment key) {
        long l = -1;
        long r = getNumberOfEntries(mapped);

        while (r - l > 1) {
            long mid = (l + r) / 2;
            SSTableRowInfo info = createRowInfo(ind, mid);
            if (greaterThen(info.keyOffset, info.keySize, mapped, key)) {
                l = mid;
            } else {
                r = mid;
            }
        }
        return r == getNumberOfEntries(mapped) ? -1 : r;
    }

    //return - List ordered form the latest created sstable to the first.
    public List<SSTableRowInfo> firstGreaterKeys(MemorySegment key) {
        List<SSTableRowInfo> ans = new ArrayList<>();

        for (int i = ssTables.size() - 1; i >= 0; i--) {
            long entryIndexesLine = 0;
            if (key != null) {
                entryIndexesLine = searchKeyInFile(i, ssTables.get(i), key);
             }
            if (entryIndexesLine < 0) {
                continue;
            }
            ans.add(createRowInfo(i, entryIndexesLine));
        }
        return ans;
    }

    private SSTableRowInfo createRowInfo(int ind, final long rowIndex) {
        long start = ssTables.get(ind).get(ValueLayout.JAVA_LONG_UNALIGNED, rowIndex * ONE_LINE_SIZE + Long.BYTES);
        long size = ssTables.get(ind).get(ValueLayout.JAVA_LONG_UNALIGNED, rowIndex * ONE_LINE_SIZE + Long.BYTES * 2);

        long start1 = ssTables.get(ind).get(ValueLayout.JAVA_LONG_UNALIGNED,rowIndex * ONE_LINE_SIZE + Long.BYTES * 3);
        long size1 = ssTables.get(ind).get(ValueLayout.JAVA_LONG_UNALIGNED,rowIndex * ONE_LINE_SIZE + Long.BYTES * 4);
        return new SSTableRowInfo(start, size, start1, size1, ind, rowIndex);
    }

    public SSTableRowInfo searchInSStables(MemorySegment key) {
        for (int i = ssTables.size() - 1; i >= 0; i--) {
            long ind = searchKeyInFile(i, ssTables.get(i), key);
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
        for (long t = info.rowShift + 1; t < getNumberOfEntries(ssTables.get(info.ssTableInd)); t++) {
            var inf = createRowInfo(info.ssTableInd, t);

            Entry<MemorySegment> row = getRow(inf);
            if (segComp.compare(row.key(), maxKey) < 0) {
                return inf;
            }
        }
        return null;
    }

    private long getNumberOfEntries(MemorySegment memSeg) {
        return memSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public void dumpIterator(Iterable<Entry<MemorySegment>> iter) throws IOException {
        Iterator<Entry<MemorySegment>> iter1 = iter.iterator();

        if (ssTablesDir == null || !iter1.hasNext()) {
            closeArena();
            return;
        }
        String suff = String.valueOf(Utils.getMaxNumberOfFile(ssTablesDir, SS_TABLE_COMMON_PREF) + 1);

        final Path tmpFile = ssTablesDir.resolve("data.tmp");
        final Path targetFile = ssTablesDir.resolve(SS_TABLE_COMMON_PREF + suff);

        try {
            Files.createFile(ssTablesDir.resolve(SS_TABLE_COMMON_PREF + suff));
        } catch (FileAlreadyExistsException ignored) {
            // it is ok, actually it is normal state
        }

        try (FileChannel ssTableChannel =
                     FileChannel.open(tmpFile, options);
             Arena saveArena = Arena.ofConfined()) {

            long ssTableLenght = 0L;
            long indexLength = 0L;

            while (iter1.hasNext()) {
                var seg = iter1.next();
                ssTableLenght += seg.key().byteSize() + getValueOrNull(seg).byteSize();
                indexLength += ONE_LINE_SIZE;
            }
            long currOffsetSSTable = 0L;

            MemorySegment mappedSSTable = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, currOffsetSSTable, ssTableLenght + indexLength + Long.BYTES,
                    saveArena);

            currOffsetSSTable = Utils.dumpLong(mappedSSTable, indexLength / ONE_LINE_SIZE, currOffsetSSTable);

            long shiftForData = indexLength + Long.BYTES;

            for (Entry<MemorySegment> kv : iter) {
                // key offset
                currOffsetSSTable = Utils.dumpLong(mappedSSTable, shiftForData, currOffsetSSTable);
                // key length
                currOffsetSSTable = Utils.dumpLong(mappedSSTable, kv.key().byteSize(), currOffsetSSTable);
                shiftForData += kv.key().byteSize();

                // value offset
                currOffsetSSTable = Utils.dumpLong(mappedSSTable, shiftForData, currOffsetSSTable);
                // value length
                currOffsetSSTable = Utils.dumpLong(mappedSSTable, Utils.rightByteSize(kv), currOffsetSSTable);
                shiftForData += getValueOrNull(kv).byteSize();
            }

            for (Entry<MemorySegment> kv : iter) {
                currOffsetSSTable = Utils.dumpSegment(mappedSSTable, kv.key(), currOffsetSSTable);
                currOffsetSSTable = Utils.dumpSegment(mappedSSTable, getValueOrNull(kv), currOffsetSSTable);
            }

            Files.move(tmpFile, targetFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        } finally {
            closeArena();
        }
    }

    private void closeArena() {
        if (!isClosedArena) {
            arenaForReading.close();
        }
        isClosedArena = true;
    }

    public void deleteAllOldFiles() throws IOException {
        closeArena();
        Utils.deleteFiles(ssTablesPaths);
    }
}
