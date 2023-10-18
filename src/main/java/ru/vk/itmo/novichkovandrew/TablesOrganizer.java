package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.MergeIterator;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;
import ru.vk.itmo.novichkovandrew.table.AbstractTable;
import ru.vk.itmo.novichkovandrew.table.MemTable;
import ru.vk.itmo.novichkovandrew.table.SSTable;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static ru.vk.itmo.novichkovandrew.Utils.copyToSegment;

/**
 * Class, that can organize different sst tables.
 */
public class TablesOrganizer implements Closeable {
    /**
     * Path associated with SSTables.
     */
    private final Path path;
    private final List<AbstractTable> tables;
    private final MemTable memTable;

    private final StandardOpenOption[] openOptions = new StandardOpenOption[]{
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
    };

    public TablesOrganizer(Path path, MemTable memTable) {
        this.memTable = memTable;
        this.path = path;
        this.tables = createTablesList();
    }

    public Iterator<Entry<MemorySegment>> mergeIterator(MemorySegment from, boolean fromInclusive,
                                                        MemorySegment to, boolean toInclusive) {
        return new MergeIterator(
                createIteratorsList(from, fromInclusive, to, toInclusive),
                memTable.comparator()
        );
    }

    public void flushMemTable() throws IOException {
        Path sstPath = sstTablePath(tables.size());
        try (FileChannel sst = FileChannel.open(sstPath, openOptions);
             Arena arena = Arena.ofConfined()
        ) {
            long metaSize = memTable.getMetaDataSize();
            long sstOffset = 0L;
            long indexOffset = Utils.writeLong(sst, 0L, memTable.size());
            MemorySegment sstMap = sst.map(FileChannel.MapMode.READ_WRITE, metaSize, memTable.byteSize(), arena);
            for (Entry<MemorySegment> entry : memTable) {
                long keyOffset = sstOffset + metaSize;
                long valueOffset = keyOffset + entry.key().byteSize();
                valueOffset *= memTable.isTombstone(entry.key()) ? -1 : 1;
                indexOffset = writePosToFile(sst, indexOffset, keyOffset, valueOffset);
                sstOffset = copyToSegment(sstMap, entry.key(), sstOffset);
                sstOffset = copyToSegment(sstMap, entry.value(), sstOffset);
            }
            writePosToFile(sst, indexOffset, sstOffset + metaSize, 0L);
        }
    }

    @Override
    public void close() throws IOException {
        for (var table : tables) {
            table.close();
        }
        tables.clear();
    }

    private List<AbstractTable> createTablesList() {
        Stream<AbstractTable> memStream = Stream.of(memTable);
        Stream<AbstractTable> sstStream = IntStream
                .rangeClosed(1, Utils.filesCount(path))
                .mapToObj(n -> new SSTable(sstTablePath(n), n));
        return Stream.concat(memStream, sstStream).collect(Collectors.toList());
    }

    private List<TableIterator<MemorySegment>> createIteratorsList(MemorySegment from, boolean fromInclusive,
                                                                   MemorySegment to, boolean toInclusive) {
        return tables.stream()
                .map(t -> t.tableIterator(from, fromInclusive, to, toInclusive))
                .collect(Collectors.toList());
    }

    private synchronized Path sstTablePath(long suffix) {
        String fileName = String.format("data-%s.txt", suffix);
        return path.resolve(Path.of(fileName));
    }

    private long writePosToFile(FileChannel channel, long rawOffset, long keyOff, long valOff) {
        long offset = rawOffset;
        offset = Utils.writeLong(channel, offset, keyOff);
        offset = Utils.writeLong(channel, offset, valOff);
        return offset;
    }
}
