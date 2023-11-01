package ru.vk.itmo.novichkovandrew.dao;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.utils.Utils;
import ru.vk.itmo.novichkovandrew.exceptions.FileChannelException;
import ru.vk.itmo.novichkovandrew.iterator.MergeIterator;
import ru.vk.itmo.novichkovandrew.table.Footer;
import ru.vk.itmo.novichkovandrew.table.Handle;
import ru.vk.itmo.novichkovandrew.table.KeyComparator;
import ru.vk.itmo.novichkovandrew.table.MMapTableWriter;
import ru.vk.itmo.novichkovandrew.table.MemTable;
import ru.vk.itmo.novichkovandrew.table.SSTable;
import ru.vk.itmo.novichkovandrew.table.Table;
import ru.vk.itmo.novichkovandrew.table.TableWriter;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class PersistentDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    /**
     * Path associated with SSTables.
     */
    private final Path path;
    private final List<Table<MemorySegment>> tables;
    private final MemTable memTable;
    private final Comparator<MemorySegment> comparator = new KeyComparator();

    public PersistentDao(Path path) {
        this.memTable = new MemTable(comparator);
        this.path = path;
        this.tables = createAllTables();
    }

    @Override
    public void flush() {
        //TableBuilder
        Handle handle = new Handle(memTable.byteSize(), Utils.indexByteSize(memTable.rows()));
        Footer footer = new Footer(handle);
        //todo: initialize filter, metablocks, etc.
        writeIterable(Utils.sstTablePath(path, tables.size()), memTable.iterator(), footer);
    }

    @Override
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
        tables.add(new SSTable(tablePath, comparator, 1));
    }

    @Override
    public void close() throws IOException {
        if (memTable.rows() != 0) flush();
        for (var table : tables) {
            table.close();
        }
        tables.clear();
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return mergeIterator(from, true, to, false);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        return mergeIterator(key, true, key, true).next();
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.upsert(entry);
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

    private Iterator<Entry<MemorySegment>> mergeIterator(MemorySegment from, boolean fromInclusive,
                                                         MemorySegment to, boolean toInclusive) {
        var iterators = tables.stream()
                .map(t -> t.tableIterator(from, fromInclusive, to, toInclusive))
                .collect(Collectors.toList());
        return new MergeIterator(iterators, comparator);
    }

    private List<Table<MemorySegment>> createAllTables() {
        var memStream = Stream.of(memTable);
        var sstStream = IntStream
                .rangeClosed(1, Utils.filesCount(path))
                .mapToObj(n -> new SSTable(Utils.sstTablePath(path, n), comparator, n));
        return Stream.concat(memStream, sstStream).collect(Collectors.toList());
    }

    private void deleteAllTables() {
        tables.forEach(Table::clear);
        tables.clear();
    }
}
