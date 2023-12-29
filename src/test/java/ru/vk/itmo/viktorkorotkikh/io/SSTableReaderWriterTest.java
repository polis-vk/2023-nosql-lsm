package ru.vk.itmo.viktorkorotkikh.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.BaseTest;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;
import ru.vk.itmo.test.viktorkorotkikh.FactoryImpl;
import ru.vk.itmo.viktorkorotkikh.SSTable;
import ru.vk.itmo.viktorkorotkikh.compressor.LZ4Compressor;
import ru.vk.itmo.viktorkorotkikh.decompressor.LZ4Decompressor;
import ru.vk.itmo.viktorkorotkikh.io.read.AbstractSSTableReader;
import ru.vk.itmo.viktorkorotkikh.io.read.BaseSSTableReader;
import ru.vk.itmo.viktorkorotkikh.io.read.CompressedSSTableReader;
import ru.vk.itmo.viktorkorotkikh.io.write.AbstractSSTableWriter;
import ru.vk.itmo.viktorkorotkikh.io.write.BaseSSTableWriter;
import ru.vk.itmo.viktorkorotkikh.io.write.CompressedSSTableWriter;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;

class SSTableReaderWriterTest extends BaseTest {
    private static final int BLOCK_4KB = 4 * 1024;

    private void testReader(
            AbstractSSTableReader reader,
            List<? extends Entry<MemorySegment>> entries,
            DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> factory
    ) throws Exception {
        for (Entry<MemorySegment> entry : entries) {
            Entry<String> stringEntry = new BaseEntry<>(factory.toString(entry.key()), factory.toString(entry.value()));
            Entry<MemorySegment> entryFromReader = reader.get(entry.key());
            Entry<String> stringEntryFromReader = new BaseEntry<>(factory.toString(entryFromReader.key()), factory.toString(entryFromReader.value()));
            Assertions.assertEquals(stringEntry, stringEntryFromReader);
        }

        Iterator<Entry<MemorySegment>> readerIterator = reader.iterator(entries.getFirst().key(), entries.getLast().key());
        int readerEntriesSize = 0;
        while (readerIterator.hasNext()) {
            Entry<MemorySegment> entryFromReader = readerIterator.next();
            Entry<String> stringEntryFromReader = new BaseEntry<>(factory.toString(entryFromReader.key()), factory.toString(entryFromReader.value()));
            Assertions.assertTrue(readerEntriesSize < entries.size());
            Entry<MemorySegment> entry = entries.get(readerEntriesSize);
            Entry<String> stringEntry = new BaseEntry<>(factory.toString(entry.key()), factory.toString(entry.value()));
            Assertions.assertEquals(stringEntry, stringEntryFromReader);
            readerEntriesSize++;
        }
        Assertions.assertEquals(entries.size() - 1, readerEntriesSize);
        readerIterator = reader.iterator(entries.getLast().key(), null);
        readerEntriesSize = 0;
        while (readerIterator.hasNext()) {
            Entry<MemorySegment> entryFromReader = readerIterator.next();
            Entry<String> stringEntryFromReader = new BaseEntry<>(factory.toString(entryFromReader.key()), factory.toString(entryFromReader.value()));
            Entry<String> stringEntry = new BaseEntry<>(factory.toString(entries.getLast().key()), factory.toString(entries.getLast().value()));
            Assertions.assertEquals(stringEntry, stringEntryFromReader);
            readerEntriesSize++;
        }
        Assertions.assertEquals(1, readerEntriesSize);
    }

    @Test
    void writeEntriesAndReadSameBaseTest() throws Exception {
        AbstractSSTableWriter writer = new BaseSSTableWriter();
        Path baseDir = Files.createTempDirectory("dao");
        DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> factory = new FactoryImpl();
        List<? extends Entry<MemorySegment>> entries = entries(1000).stream().map(entry ->
                new BaseEntry<>(factory.fromString(entry.key()), factory.fromString(entry.value()))
        ).toList();
        writer.write(entries::iterator, baseDir, 0);
        try (
                Arena arena = Arena.ofConfined();
                FileChannel ssTableFileChannel = FileChannel.open(
                        SSTable.dataName(baseDir, 0),
                        StandardOpenOption.READ
                );
                FileChannel indexFileChannel = FileChannel.open(
                        SSTable.indexName(baseDir, 0),
                        StandardOpenOption.READ
                );
                FileChannel compressionInfoFileChannel = FileChannel.open(
                        SSTable.compressionInfoName(baseDir, 0),
                        StandardOpenOption.READ
                )
        ) {
            MemorySegment mappedSSTable = ssTableFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Files.size(SSTable.dataName(baseDir, 0)),
                    arena
            );
            MemorySegment mappedIndexFile = indexFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Files.size(SSTable.indexName(baseDir, 0)),
                    arena
            );
            MemorySegment mappedCompressionInfo = compressionInfoFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Files.size(SSTable.compressionInfoName(baseDir, 0)),
                    arena
            );
            AbstractSSTableReader reader = new BaseSSTableReader(mappedSSTable, mappedIndexFile, mappedCompressionInfo, 0);
            testReader(reader, entries, factory);
        }
    }

    @Test
    void writeEntriesAndReadSameCompressedTest() throws Exception {
        AbstractSSTableWriter writer = new CompressedSSTableWriter(new LZ4Compressor(), BLOCK_4KB);
        Path baseDir = Files.createTempDirectory("dao");
        DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> factory = new FactoryImpl();
        List<? extends Entry<MemorySegment>> entries = entries(1000).stream().map(entry ->
                new BaseEntry<>(factory.fromString(entry.key()), factory.fromString(entry.value()))
        ).toList();
        writer.write(entries::iterator, baseDir, 0);
        try (
                Arena arena = Arena.ofConfined();
                FileChannel ssTableFileChannel = FileChannel.open(
                        SSTable.dataName(baseDir, 0),
                        StandardOpenOption.READ
                );
                FileChannel indexFileChannel = FileChannel.open(
                        SSTable.indexName(baseDir, 0),
                        StandardOpenOption.READ
                );
                FileChannel compressionInfoFileChannel = FileChannel.open(
                        SSTable.compressionInfoName(baseDir, 0),
                        StandardOpenOption.READ
                )
        ) {
            MemorySegment mappedSSTable = ssTableFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Files.size(SSTable.dataName(baseDir, 0)),
                    arena
            );
            MemorySegment mappedIndexFile = indexFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Files.size(SSTable.indexName(baseDir, 0)),
                    arena
            );
            MemorySegment mappedCompressionInfo = compressionInfoFileChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    Files.size(SSTable.compressionInfoName(baseDir, 0)),
                    arena
            );
            AbstractSSTableReader reader
                    = new CompressedSSTableReader(mappedSSTable, mappedIndexFile, mappedCompressionInfo, new LZ4Decompressor(), 0);
            testReader(reader, entries, factory);
        }
    }
}
