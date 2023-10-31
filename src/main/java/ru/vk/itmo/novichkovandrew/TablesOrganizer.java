package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.exceptions.FileChannelException;
import ru.vk.itmo.novichkovandrew.iterator.MergeIterator;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;
import ru.vk.itmo.novichkovandrew.table.*;

import java.io.Closeable;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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

    public void flushMemTable() {
        Handle handle = new Handle(memTable.byteSize(), Utils.indexByteSize(memTable.rows()));
        Footer footer = new Footer(handle);
        //todo: initialize filter, metablocks, etc.
        writeIterable(Utils.sstTablePath(path, tables.size()), memTable.iterator(), footer);
    }

    private void writeIterable(Path iterablePath, Iterator<Entry<MemorySegment>> iterator, Footer footer) {
        try (TableWriter writer = new MMapTableWriter(iterablePath, footer)) {
            while (iterator.hasNext()) {
                var entry = iterator.next();
                writer.writeIndexHandle(entry);
                writer.writeEntry(entry);
            }
            writer.writeIndexHandle(Utils.EMPTY);
            writer.writeFooter(footer);
        } catch (IOException ex) {
            throw new FileChannelException("Invalid initialize writer", ex);
        }
    }


    @Override
    public void close() throws IOException {
        for (var table : tables) {
            table.close();
        }
        tables.clear();
    }

    public void compact() throws IOException {
        Path compactPath = path.resolve("temp.txt");
        Files.createFile(compactPath);
        var iterator = mergeIterator(null, true, null, true);
        long rows = tables.stream().mapToLong(Table::rows).sum();
        long byteSize = tables.stream().mapToLong(Table::byteSize).sum();
        Footer footer = new Footer(new Handle(byteSize, Utils.indexByteSize(rows)));
        writeIterable(compactPath, iterator, footer);
        Path tablePath = Utils.sstTablePath(path, 1);
        deleteAllTables();
        Files.move(compactPath, tablePath);
        tables.add(memTable);
        tables.add(new SSTable(tablePath, 1));
    }


    private void deleteAllTables() {
        tables.forEach(AbstractTable::clear);
        tables.clear();
    }

    private List<AbstractTable> createTablesList() {
        Stream<AbstractTable> memStream = Stream.of(memTable);
        Stream<AbstractTable> sstStream = IntStream
                .rangeClosed(1, Utils.filesCount(path))
                .mapToObj(n -> new SSTable(Utils.sstTablePath(path, n), n));
        return Stream.concat(memStream, sstStream).collect(Collectors.toList());
    }

    private List<TableIterator<MemorySegment>> createIteratorsList(MemorySegment from, boolean fromInclusive,
                                                                   MemorySegment to, boolean toInclusive) {
        return tables.stream()
                .map(t -> t.tableIterator(from, fromInclusive, to, toInclusive))
                .collect(Collectors.toList());
    }


}
