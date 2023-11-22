package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class SSTableManager implements DaoFileGet<MemorySegment, Entry<MemorySegment>>, AutoCloseable {

    public static final String SSTABLE_NAME = "sstable";

    private final Path root;
    private final Arena arena;
    private final Deque<SSTable> ssTables;

    public SSTableManager(final Path root) throws IOException {
        this.root = root;
        this.arena = Arena.ofShared();
        this.ssTables = readTables();
    }

    private Deque<SSTable> readTables() throws IOException {
        final Deque<SSTable> tables = new ArrayDeque<>();
        SSTable table;
        while ((table = readTable(getNextSSTableName(tables.size()))) != null) {
            tables.add(table);
        }
        return tables.reversed();
    }

    private SSTable readTable(final String name) throws IOException {
        return SSTable.create(root, name, arena);
    }

    private String getNextSSTableName() {
        return getNextSSTableName(ssTables.size());
    }

    private String getNextSSTableName(final int size) {
        return SSTABLE_NAME + size;
    }

    public synchronized void write(SortedMap<MemorySegment, Entry<MemorySegment>> map) throws IOException {
        if (map.isEmpty()) {
            return;
        }
        final String name = getNextSSTableName();
        SSTable.write(map, root, name);
        ssTables.addFirst(SSTable.create(root, name, arena));
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) throws IOException {

        List<PriorityShiftedIterator<Entry<MemorySegment>>> iterators = new ArrayList<>();
        int i = 0;
        for (final SSTable ssTable : ssTables) {
            iterators.add(new MemEntryPriorityIterator(i, ssTable.get(from, to)));
            i++;
        }

        return new MergeEntryIterator(iterators);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) throws IOException {
        Entry<MemorySegment> value = null;
        for (final SSTable ssTable: ssTables) {
            value = ssTable.get(key);
            if (value != null) {
                if (value.value() == null) {
                    value = null;
                }
                break;
            }
        }
        return value;
    }

    private SizeInfo getTotalInfoSize() {
        final SizeInfo result = new SizeInfo();
        for (SSTable ssTable: ssTables) {
            result.add(ssTable.getSizeInfo());
        }
        return result;
    }

    public synchronized void compact() throws IOException {
        if (ssTables.size() <= 1) {
            return;
        }
        Path tableTmpPath = null;
        Path indexTmpPath = null;
        final SizeInfo sizes = getTotalInfoSize();
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
                        new MemEntryPriorityIterator(0, get(null, null))
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

            for (SSTable ssTable : ssTables) {
                ssTable.delete();
            }
            ssTables.clear();

            final String sstableName = getNextSSTableName();
            final Path dataPath = SSTable.getDataPath(root, sstableName);
            final Path indexPath = SSTable.getIndexPath(root, sstableName);
            Files.copy(tableTmpPath, dataPath);
            try {
                Files.copy(indexTmpPath, indexPath);
            } catch (IOException e) {
                Files.deleteIfExists(dataPath);
                throw e;
            }
            ssTables.addFirst(SSTable.create(root, sstableName, arena));
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
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }

        ssTables.clear();
        arena.close();
    }
}
