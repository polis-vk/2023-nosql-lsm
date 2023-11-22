package ru.vk.itmo.kovalevigor;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import ru.vk.itmo.Entry;

public class SSTableManager implements DaoFileGet<MemorySegment, Entry<MemorySegment>>, AutoCloseable {

    public static final String SSTABLE_NAME = "sstable";

    private final Path root;
    private final Arena arena;
    private final BlockingDeque<SSTable> ssTables;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public SSTableManager(final Path root) throws IOException {
        this.root = root;
        this.arena = Arena.ofShared();
        this.ssTables = readTables();
    }

    private BlockingDeque<SSTable> readTables() throws IOException {
        final List<SSTable> tables = new ArrayList<>();
        SSTable table;
        while ((table = readTable(getNextSSTableName(tables.size()))) != null) {
            tables.add(table);
        }
        return new LinkedBlockingDeque<>(tables.reversed());
    }

    private SSTable readTable(final String name) throws IOException {
        return SSTable.create(root, name, arena);
    }

    private static String getNextSSTableName(final Collection<SSTable> ssTables) {
        return getNextSSTableName(ssTables.size());
    }

    private static String getNextSSTableName(final int size) {
        return SSTABLE_NAME + size;
    }

    public synchronized void write(SortedMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        if (map.isEmpty()) {
            return;
        }
        final Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            final String name = getNextSSTableName(ssTables);
            SSTable.write(map, root, name);
            ssTables.addFirst(SSTable.create(root, name, arena));
        } finally {
            writeLock.unlock();
        }
    }

    public static Iterator<Entry<MemorySegment>> get(
            Collection<SSTable> ssTables,
            final MemorySegment from,
            final MemorySegment to
    ) throws IOException {
        List<PriorityShiftedIterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        int i = 0;
        for (final SSTable ssTable : ssTables) {
            iterators.add(new MemEntryPriorityIterator(i, ssTable.get(from, to)));
            i++;
        }
        return new MergeEntryIterator(iterators);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) throws IOException {
        final Lock readLock = lock.readLock();
        readLock.lock();
        try {
            return get(ssTables, from, to);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) throws IOException {
        final Lock readLock = lock.readLock();
        readLock.lock();
        try {
            Entry<MemorySegment> value = null;
            for (final SSTable ssTable : ssTables) {
                value = ssTable.get(key);
                if (value != null) {
                    if (value.value() == null) {
                        value = null;
                    }
                    break;
                }
            }
            return value;
        } finally {
            readLock.unlock();
        }
    }

    private static SizeInfo getTotalInfoSize(final Collection<SSTable> ssTables) {
        final SizeInfo result = new SizeInfo();
        for (SSTable ssTable: ssTables) {
            result.add(ssTable.getSizeInfo());
        }
        return result;
    }

    public synchronized void compact() throws IOException {
        List<SSTable> compactTables = new ArrayList<>(ssTables);
        if (compactTables.size() <= 1) {
            return;
        }
        Path tableTmpPath = null;
        Path indexTmpPath = null;
        final SizeInfo sizes = getTotalInfoSize(compactTables);
        try {
            tableTmpPath = Files.createTempFile(null, null);
            indexTmpPath = Files.createTempFile(null, null);
            final SizeInfo realSize = new SizeInfo();
            try (Arena tmpArena = Arena.ofConfined(); SStorageDumper dumper = new SStorageDumper(
                    sizes,
                    tableTmpPath,
                    indexTmpPath,
                    tmpArena
            )) {
                final Iterator<Entry<MemorySegment>> iterator = new MergeEntryIterator(List.of(
                        new MemEntryPriorityIterator(0, get(compactTables, null, null))
                ));
                while (iterator.hasNext()) {
                    final Entry<MemorySegment> entry = iterator.next();
                    if (entry.value() != null) {
                        dumper.writeEntry(entry);
                        realSize.size += 1;
                        realSize.keysSize += entry.key().byteSize();
                        realSize.valuesSize += entry.value().byteSize();
                    }
                }
                dumper.setKeysSize(realSize.keysSize);
                dumper.setValuesSize(realSize.valuesSize);
            }

            try (FileChannel dataChannel = FileChannel.open(tableTmpPath, StandardOpenOption.WRITE);
                 FileChannel indexChannel = FileChannel.open(indexTmpPath, StandardOpenOption.WRITE)
            ) {
                dataChannel.truncate(SStorageDumper.getSize(realSize.keysSize, realSize.valuesSize));
                indexChannel.truncate(SStorageDumper.getIndexSize(realSize.size));
            }
        final Lock writeLock = lock.writeLock();
        writeLock.lock();
        try {
            for (SSTable ssTable : compactTables) {
                ssTable.delete();
            }

            final String sstableName = getNextSSTableName(Collections.emptyList());
            final Path dataPath = SSTable.getDataPath(root, sstableName);
            final Path indexPath = SSTable.getIndexPath(root, sstableName);
            Files.copy(tableTmpPath, dataPath);
            try {
                Files.copy(indexTmpPath, indexPath);
            } catch (IOException e) {
                Files.deleteIfExists(dataPath);
                throw e;
            }
                for (int i = 0; i < compactTables.size(); i++) {
                    ssTables.pollLast();
                }
                List<SSTable> movedSStables = new ArrayList<>(ssTables);
                ssTables.clear();
                ssTables.add(SSTable.create(root, sstableName, arena));
                for (final SSTable ssTable : movedSStables.reversed()) {
                    ssTable.move(root, getNextSSTableName(ssTables));
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            if (tableTmpPath != null) {
                Files.deleteIfExists(tableTmpPath);
            }
            if (indexTmpPath != null) {
                Files.deleteIfExists(indexTmpPath);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {

        if (!arena.scope().isAlive()) {
            return;
        }

        ssTables.clear();
        arena.close();
    }
}
