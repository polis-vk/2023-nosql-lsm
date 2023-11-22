package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static ru.vk.itmo.test.kachmareugene.Utils.getValueOrNull;

public class SSTablesController {
    private final Path ssTablesDir;
    // TODO add pair in key
    public final Map<Path, AtomicInteger> filesLifeStatistics = new ConcurrentHashMap<>();
    public final AtomicLong maximumFileNum;
    private final List<Pair<Path, MemorySegment>> ssTables = new CopyOnWriteArrayList<>();
    private static final String SS_TABLE_COMMON_PREF = "ssTable";
    //  index format: (long) keyOffset, (long) keyLen, (long) valueOffset, (long) valueLen
    private static final long ONE_LINE_SIZE = 4 * Long.BYTES;
    private final Arena arenaForReading = Arena.ofShared();
    private boolean isClosedArena;
    private final Comparator<MemorySegment> segComp;

    public SSTablesController(Path dir, Comparator<MemorySegment> com) {
        this.ssTablesDir = dir;
        this.segComp = com;
        this.maximumFileNum = new AtomicLong(Utils.getMaxNumberOfFile(ssTablesDir, SS_TABLE_COMMON_PREF));
        ssTables.addAll(Utils.openFiles(dir, SS_TABLE_COMMON_PREF,
                filesLifeStatistics, arenaForReading));
    }

    // fixme add CAS to add
    public SSTablesController(Comparator<MemorySegment> com) {
        this.ssTablesDir = null;
        this.maximumFileNum = new AtomicLong(0);
        this.segComp = com;
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
    public List<SSTableRowInfo> firstGreaterKeys(MemorySegment key, long smallestNum) {
        List<SSTableRowInfo> ans = new ArrayList<>();

        var ssTablesCopy = new ArrayList<>(ssTables);
        Collections.reverse(ssTablesCopy);

        for (var file : ssTablesCopy) {
            int i = (int) Utils.getNumberFromFileName(file.first, SS_TABLE_COMMON_PREF);

            if (i < smallestNum) {
                continue;
            }

            long entryIndexesLine = 0;
            if (key != null) {
                entryIndexesLine = searchKeyInFile(i, file.second, key);
            }
            if (entryIndexesLine < 0) {
                continue;
            }
            ans.add(createRowInfo(i, entryIndexesLine));
            filesLifeStatistics.merge(file.first,
                    new AtomicInteger(1), (old, n) -> new AtomicInteger(old.addAndGet(n.get())));
        }
        return ans;
    }

    private SSTableRowInfo createRowInfo(int realFileIndex, final long rowIndex) {
        MemorySegment seg = Utils.findMemSegInListOfFiles(ssTables, realFileIndex, SS_TABLE_COMMON_PREF);

        if (seg.equals(MemorySegment.NULL)) {
            return new SSTableRowInfo(-1, -1, -1, -1, realFileIndex, rowIndex);
        }

        long start = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, rowIndex * ONE_LINE_SIZE + Long.BYTES);
        long size = seg.get(ValueLayout.JAVA_LONG_UNALIGNED, rowIndex * ONE_LINE_SIZE + Long.BYTES * 2);

        long start1 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED,rowIndex * ONE_LINE_SIZE + Long.BYTES * 3);
        long size1 = seg.get(ValueLayout.JAVA_LONG_UNALIGNED,rowIndex * ONE_LINE_SIZE + Long.BYTES * 4);
        return new SSTableRowInfo(start, size, start1, size1, realFileIndex, rowIndex);
    }

    public SSTableRowInfo searchInSStables(MemorySegment key) {
        var ssTablesCopy = new ArrayList<>(ssTables);
        Collections.reverse(ssTablesCopy);

        for (var file : ssTablesCopy) {
            int i = (int) Utils.getNumberFromFileName(file.first, SS_TABLE_COMMON_PREF);
            long ind = searchKeyInFile(i, file.second, key);

            if (ind >= 0) {
                return createRowInfo(i, ind);
            }
        }
        return null;
    }

    public Entry<MemorySegment> getRow(SSTableRowInfo info) {
        if (info == null || !info.isValidInfo()) {
            return null;
        }
        MemorySegment memSeg = Utils.findMemSegInListOfFiles(ssTables, info.ssTableInd, SS_TABLE_COMMON_PREF);

        var key = memSeg.asSlice(info.keyOffset, info.keySize);

        if (info.isDeletedData()) {
            return new BaseEntry<>(key, null);
        }
        var value = memSeg.asSlice(info.valueOffset, info.getValueSize());

        return new BaseEntry<>(key, value);
    }

    /**
     * Ignores deleted values.
     */
    public SSTableRowInfo getNextInfo(SSTableRowInfo info, MemorySegment maxKey) {
        for (long t = info.rowShift + 1; t < getNumberOfEntries(Utils.findMemSegInListOfFiles(ssTables,
                info.ssTableInd, SS_TABLE_COMMON_PREF)); t++) {
            var inf = createRowInfo(info.ssTableInd, t);

            Entry<MemorySegment> row = getRow(inf);
            if (segComp.compare(row.key(), maxKey) < 0) {
                if (inf == null) {
                    decrease(info.ssTableInd);
                }
                return inf;
            }
        }
        decrease(info.ssTableInd);
        return null;
    }

    private long getNumberOfEntries(MemorySegment memSeg) {
        return memSeg.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    public Pair<Path, Long> dumpIterator(Iterable<Entry<MemorySegment>> iter) throws IOException {
        Iterator<Entry<MemorySegment>> iter1 = iter.iterator();

        if (ssTablesDir == null || !iter1.hasNext()) {
            return new Pair<>(null, null);
        }


        long curMaxFileNum = maximumFileNum.incrementAndGet();
        String suff = String.valueOf(curMaxFileNum);
        Set<OpenOption> options = Set.of(WRITE, READ, CREATE);

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

            Files.deleteIfExists(targetFile);

            Files.move(tmpFile, targetFile, StandardCopyOption.ATOMIC_MOVE);
            return new Pair<>(targetFile, curMaxFileNum);
        }
    }

    public void closeArena() {
        if (!isClosedArena) {
            arenaForReading.close();
        }
        isClosedArena = true;
    }
    public void addFileToLists(Path filePath) {
        Utils.openMemorySegment(filePath, ssTables,
                filesLifeStatistics, arenaForReading);
    }
    public void tryToDelete() throws IOException {
        for (var kv : filesLifeStatistics.entrySet()) {
            // TODO equal zero
            if (kv.getValue().get() <= 0) {
                int ind = Utils.getInd(ssTables, kv.getKey());
                Path toDelete = ssTables.get(ind).first;
                ssTables.remove(ind);
                Utils.deleteFile(toDelete);
            }
        }
    }
    public void decrease(long num) {
        Utils.decrease(ssTablesDir, filesLifeStatistics, SS_TABLE_COMMON_PREF, num);
    }

    public void decreaseAllSmall(long biggest) {
        Utils.decreaseAllSmall(ssTablesDir, SS_TABLE_COMMON_PREF, filesLifeStatistics, biggest);
    }
}
