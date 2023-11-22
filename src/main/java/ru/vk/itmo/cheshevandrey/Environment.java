package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static ru.vk.itmo.cheshevandrey.DiskStorage.INDEX_FILE;
import static ru.vk.itmo.cheshevandrey.Tools.createDir;
import static ru.vk.itmo.cheshevandrey.Tools.createFile;

public class Environment implements AutoCloseable {

    private final List<MemorySegment> mainSegmentList;
    private final List<MemorySegment> intermSegmentList;
    private final List<MemorySegment> secondarySegmentList;

    private final Path storagePath;
    private final Path mainDir;
    private final Path intermediateDir;
    private final Path secondaryDir;

    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memTable;
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingTable;
    private final AtomicLong memTableBytes;

    private boolean isCompacted;
    private int ssTablesCount;

    private static final String DIR_0 = "0";
    private static final String DIR_1 = "1";
    private static final String INTERMEDIATE_DIR = "tmp";

    private final Arena arena = Arena.ofShared();

    public Environment(
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> table,
            Path storagePath
    ) throws IOException {
        this.memTable = new ConcurrentSkipListMap<>(Tools::compare);
        this.flushingTable = table;
        this.memTableBytes = new AtomicLong(0);

        this.storagePath = storagePath;

        String workDir = DiskStorage.readWorkDir(storagePath);
        secondaryDir = storagePath.resolve(workDir.equals(DIR_0) ? DIR_1 : workDir);
        intermediateDir = storagePath.resolve(INTERMEDIATE_DIR);
        mainDir = storagePath.resolve(workDir);

        createDirsAndFilesIfNeeded();

        this.secondarySegmentList = loadOrRecover(secondaryDir, arena);
        this.intermSegmentList = loadOrRecover(intermediateDir, arena);
        this.mainSegmentList = loadOrRecover(mainDir, arena);
    }

    private void createDirsAndFilesIfNeeded() throws IOException {
        createDir(storagePath);

        createDir(mainDir);
        createDir(intermediateDir);
        createDir(secondaryDir);

        createFile(mainDir.resolve(INDEX_FILE));
        createFile(intermediateDir.resolve(INDEX_FILE));
        createFile(secondaryDir.resolve(INDEX_FILE));
    }

    private List<MemorySegment> loadOrRecover(Path path, Arena arena) throws IOException {
        Path indexFilePath = path.resolve(INDEX_FILE);
        List<String> files = Files.readAllLines(indexFilePath);
        ssTablesCount = files.size();

        List<MemorySegment> result = new ArrayList<>(ssTablesCount);
        for (int ssTable = 0; ssTable < ssTablesCount; ssTable++) {
            Path file = path.resolve(String.valueOf(ssTable));

            if (!Files.exists(file)) {
                continue;
            }

            long fileSize = Files.size(file);
            if (fileSize == 0) { // Попали на пустой файл "0" или "1".
                continue;
            }

            try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
                MemorySegment fileSegment = fileChannel.map(
                        FileChannel.MapMode.READ_ONLY,
                        0,
                        fileSize,
                        arena
                );
                result.add(fileSegment);
            }
        }

        return result;
    }

    public Iterator<Entry<MemorySegment>> range(
            MemorySegment from,
            MemorySegment to,
            Range range) {

        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();

        if (range == Range.DISK) {
            addIterators(iterators, intermSegmentList, from, to);
            addIterators(iterators, mainSegmentList, from, to);
        }
        if (range == Range.ALL) {
            addIterators(iterators, intermSegmentList, from, to);
            addIterators(iterators, secondarySegmentList, from, to);
            addIterators(iterators, mainSegmentList, from, to);
            iterators.add(getInMemory(from, to, false).iterator());
            iterators.add(getInMemory(from, to, true).iterator());
        }

        return new MergeIterator<>(iterators, Comparator.comparing(Entry::key, Tools::compare)) {
            @Override
            protected boolean skip(Entry<MemorySegment> memorySegmentEntry) {
                return memorySegmentEntry.value() == null;
            }
        };
    }

    private void addIterators(List<Iterator<Entry<MemorySegment>>> iterators,
                              List<MemorySegment> segmentList,
                              MemorySegment from,
                              MemorySegment to) {
        for (MemorySegment memorySegment : segmentList) {
            iterators.add(SsTableIterator.iterator(memorySegment, from, to));
        }
    }

    public Iterator<Entry<MemorySegment>> range(MemorySegment from, MemorySegment to) {
        return range(from, to, Range.ALL);
    }

    private Iterable<Entry<MemorySegment>> getInMemory(MemorySegment from, MemorySegment to, Boolean isMemTable) {
        if (from == null && to == null) {
            return (isMemTable ? memTable : flushingTable).values();
        }
        if (from == null) {
            return (isMemTable ? memTable : flushingTable).headMap(to).values();
        }
        if (to == null) {
            return (isMemTable ? memTable : flushingTable).tailMap(from).values();
        }
        return (isMemTable ? memTable : flushingTable).subMap(from, to).values();
    }

    public synchronized long put(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);

        long currSize = entry.key().byteSize();
        if (entry.value() != null) {
            currSize += entry.value().byteSize();
        }

        memTableBytes.getAndAdd(currSize);
        return memTableBytes.get();
    }

    public void flush() throws IOException {
        DiskStorage.save(flushingTable.values(), storagePath);
    }

    public void compact() throws IOException {
        int ssTablesToCompactCount = mainSegmentList.size() + intermSegmentList.size();
        if (isCompacted || ssTablesToCompactCount < 2) {
            return;
        }
        IterableDisk iterable = new IterableDisk(this);
        DiskStorage.compact(iterable, storagePath, mainDir, intermediateDir, secondaryDir, ssTablesCount);
        isCompacted = true;
    }

    public ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> getTable() {
        return memTable;
    }

    public long getBytes() {
        return memTableBytes.get();
    }

    public Entry<MemorySegment> getMemTableEntry(MemorySegment key) {
        return memTable.get(key);
    }

    public Entry<MemorySegment> getFlushingTableEntry(MemorySegment key) {
        return flushingTable.get(key);
    }

    @Override
    public void close() throws Exception {
        arena.close();
    }
}
