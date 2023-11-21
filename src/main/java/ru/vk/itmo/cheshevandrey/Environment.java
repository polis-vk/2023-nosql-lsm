package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

import static ru.vk.itmo.cheshevandrey.DiskStorage.INDEX_NAME;
import static ru.vk.itmo.cheshevandrey.DiskStorage.updateIndex;
import static ru.vk.itmo.cheshevandrey.Tools.createDir;
import static ru.vk.itmo.cheshevandrey.Tools.createFile;

public class Environment {

    private final List<MemorySegment> mainSegmentList;
    private final List<MemorySegment> secondarySegmentList;

    private Path mainDir;
    private Path secondaryDir;

    private final Path storagePath;

    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memTable;
    private ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> flushingTable;
    private AtomicLong memTableBytes;

    public Environment(
            ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> table,
            long bytes,
            Path storagePath,
            Arena arena
    ) throws IOException {
        this.memTable = table;
        this.flushingTable = new ConcurrentSkipListMap<>(Tools::compare);
        this.memTableBytes = new AtomicLong(bytes);

        this.storagePath = storagePath;

        String workDir = DiskStorage.readWorkDir(storagePath);
        mainDir = storagePath.resolve(workDir);
        secondaryDir = storagePath.resolve(workDir.equals("0") ? "1" : workDir);

        // Должны гарантировано иметь две директории.
        // В каждой должны из которых должны обязательно быть файлы "0", "1".
        // "0" - используется для копирования скомпакченного из другой директории.
        // "1" - для копирования сстебла (требуется для разрешения одновременного конфликтного выполнения флаша и компакта)
        createFilesAndDirsIfNeeded();

        this.mainSegmentList = loadOrRecover(mainDir, arena);
        this.secondarySegmentList = loadOrRecover(secondaryDir, arena);
    }

    private void createFilesAndDirsIfNeeded() throws IOException {
        try {
            Files.createDirectory(storagePath);
        } catch (FileAlreadyExistsException ignored) {
            // it's ok.
        }

        createDir(mainDir);
        createDir(secondaryDir);

        String zero = "0";
        createFile(mainDir.resolve(zero));
        createFile(secondaryDir.resolve(zero));
        String one = "1";
        createFile(mainDir.resolve(one));
        createFile(secondaryDir.resolve(one));

        Path mainIndexPath = mainDir.resolve(INDEX_NAME);
        Path secondaryIndexPath = secondaryDir.resolve(INDEX_NAME);

        List<String> files = new ArrayList<>(2);
        files.add(zero);
        files.add(one);
        if (!Files.exists(mainIndexPath)) {
            updateIndex(files, mainDir);
        }
        if (!Files.exists(secondaryIndexPath)) {
            updateIndex(files, secondaryDir);
        }
    }

    private List<MemorySegment> loadOrRecover(Path path, Arena arena) throws IOException {
        Path indexFilePath = path.resolve(INDEX_NAME);
        List<String> files = Files.readAllLines(indexFilePath);
        int ssTablesNumber = files.size();

        List<MemorySegment> result = new ArrayList<>(ssTablesNumber);
        for (int ssTable = 0; ssTable < ssTablesNumber; ssTable++) {
            Path file = path.resolve(String.valueOf(ssTable));

            // Можем иметь несуществующие файлы записанные в индекс.
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
            Iterator<Entry<MemorySegment>> firstIterator,
            Iterator<Entry<MemorySegment>> secondIterator,
            MemorySegment from,
            MemorySegment to,
            Range range) {

        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        addIterators(iterators, secondarySegmentList, from, to);
        if (range == Range.ALL) {
            addIterators(iterators, mainSegmentList, from, to);
            iterators.add(secondIterator);
            iterators.add(firstIterator);
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
        Iterator<Entry<MemorySegment>> memTableIterator = getInMemory(from, to, true).iterator();
        Iterator<Entry<MemorySegment>> flushingIterator = getInMemory(from, to, false).iterator();

        return range(memTableIterator, flushingIterator, from, to, Range.ALL);
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
        this.flushingTable = memTable;
        this.memTable = new ConcurrentSkipListMap<>(Tools::compare);
        this.memTableBytes = new AtomicLong(0);

        DiskStorage.save(flushingTable.values(), storagePath);
    }

    public void compact() throws IOException {
        IterableDisk iterable = new IterableDisk(this, Range.DISK);
        DiskStorage.compact(iterable, storagePath, mainDir, secondaryDir, mainSegmentList.size());
    }

    public void completeCompactIfNeeded() throws IOException {
        DiskStorage.completeCompactIfNeeded();
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
}
