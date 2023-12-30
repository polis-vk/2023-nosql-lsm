package ru.vk.itmo.viktorkorotkikh.io.write;

import ru.vk.itmo.Entry;
import ru.vk.itmo.viktorkorotkikh.SSTable;
import ru.vk.itmo.viktorkorotkikh.io.ByteArraySegment;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.function.Supplier;

public abstract class AbstractSSTableWriter {
    private static final int BUFFER_SIZE = 64 * 1024;
    protected final ByteArraySegment longBuffer = new ByteArraySegment(Long.BYTES);
    protected final ByteArraySegment blobBuffer;
    protected final int blockSize;

    private static final long INDEX_METADATA_SIZE = Long.BYTES + 1L; // entries size + hasNoTombstones flag

    private static final long COMPRESSION_INFO_METADATA_SIZE = 2L * Integer.BYTES; // blocksCount|blockSize

    protected AbstractSSTableWriter(int blockSize) {
        this.blockSize = blockSize;
        if (this.blockSize <= 0) { // set default blob buffer
            blobBuffer = new ByteArraySegment(512);
        } else {
            blobBuffer = new ByteArraySegment(blockSize, false);
        }
    }

    public void write(
            boolean isCompacted,
            Supplier<? extends Iterator<? extends Entry<MemorySegment>>> iteratorSupplier,
            final Path baseDir,
            final int fileIndex
    ) throws IOException {
        // Write to temporary files
        final Path tempCompressionInfo = SSTable.tempCompressionInfoName(isCompacted, baseDir, fileIndex);
        final Path tempIndexName = SSTable.tempIndexName(isCompacted, baseDir, fileIndex);
        final Path tempDataName = SSTable.tempDataName(isCompacted, baseDir, fileIndex);

        // Delete temporary files to eliminate tails
        Files.deleteIfExists(tempIndexName);
        Files.deleteIfExists(tempDataName);

        // Iterate in a single pass!
        // Will write through FileChannel despite extra memory copying and
        // no buffering (which may be implemented later).
        // Looking forward to MemorySegment facilities in FileChannel!
        try (OutputStream compressionInfo =
                     new BufferedOutputStream(
                             new FileOutputStream(
                                     tempCompressionInfo.toFile()),
                             BUFFER_SIZE);
             OutputStream index =
                     new BufferedOutputStream(
                             new FileOutputStream(
                                     tempIndexName.toFile()),
                             BUFFER_SIZE);
             OutputStream data =
                     new BufferedOutputStream(
                             new FileOutputStream(
                                     tempDataName.toFile()),
                             BUFFER_SIZE)) {
            index.write(new byte[(int) INDEX_METADATA_SIZE]); // write 0, fill in the data later

            Iterator<? extends Entry<MemorySegment>> entries = iteratorSupplier.get();

            // Iterate and serialize
            // compression info:
            // isCompressed|algorithm|blocksCount|blockSize|block1Offset|block2Offset|...|blockNOffset
            // index:
            // keyNBlockNumber - номер блока для начала ключа номер N (key1Size|key1|value1Size|value1)
            // keyNSizeBlockOffset - смещение начала размера ключа внутри блока
            // hasNoTombstones|entriesSize|key1BlockNumber|key1SizeBlockOffset|key2BlockNumber|key2SizeBlockOffset|...|keyNBlockNumber|keyNSizeBlockOffset|
            // data:
            // block1|block2|...|blockN

            writeCompressionHeader(compressionInfo);
            compressionInfo.write(new byte[(int) COMPRESSION_INFO_METADATA_SIZE]); // write 0, fill in the data later

            int entriesSize = 0;
            boolean hasNoTombstones = true;

            while (entries.hasNext()) {
                // Then write the entry
                final Entry<MemorySegment> entry = entries.next();
                hasNoTombstones = entry.value() != null;
                writeEntry(entry, data, compressionInfo, index);
                entriesSize++;
            }

            index.flush();
            compressionInfo.flush();
            finish(data, compressionInfo);
            try (Arena arena = Arena.ofConfined();
                 FileChannel indexFileChannel = FileChannel.open(
                         tempIndexName,
                         StandardOpenOption.READ,
                         StandardOpenOption.WRITE
                 );
                 FileChannel compressionInfoFileChannel = FileChannel.open(
                         tempCompressionInfo,
                         StandardOpenOption.READ,
                         StandardOpenOption.WRITE
                 )
            ) {
                MemorySegment mappedIndexFile = indexFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        0L,
                        INDEX_METADATA_SIZE,
                        arena
                );

                MemorySegment mappedCompressionInfoFile = compressionInfoFileChannel.map(
                        FileChannel.MapMode.READ_WRITE,
                        2L, // isCompressed|algorithm
                        COMPRESSION_INFO_METADATA_SIZE,
                        arena
                );
                writeIndexInfo(mappedIndexFile, mappedCompressionInfoFile, entriesSize, hasNoTombstones);
                mappedIndexFile.force();
                mappedCompressionInfoFile.force();
            }
        }

        // Publish files atomically
        // FIRST index, LAST data
        final Path compressionInfoName = SSTable.compressionInfoName(isCompacted, baseDir, fileIndex);
        Files.move(
                tempCompressionInfo,
                compressionInfoName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
        final Path indexName = SSTable.indexName(isCompacted, baseDir, fileIndex);
        Files.move(
                tempIndexName,
                indexName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
        final Path dataName = SSTable.dataName(isCompacted, baseDir, fileIndex);
        Files.move(
                tempDataName,
                dataName,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
        );
    }

    protected abstract void writeIndexInfo(
            MemorySegment mappedIndexFile,
            MemorySegment mappedCompressionInfoFile,
            int entriesSize,
            boolean hasNoTombstones
    );

    protected abstract void writeEntry(
            final Entry<MemorySegment> entry,
            final OutputStream os,
            final OutputStream compressionInfoStream,
            final OutputStream indexStream
    ) throws IOException;

    /**
     * Flush blobBuffer to outputStream forcibly.
     *
     * @param os outputStream for writing
     * @param compressionInfoStream compression info outputStream
     * @throws IOException if an I/O error occurs.
     */
    protected abstract void finish(OutputStream os, OutputStream compressionInfoStream) throws IOException;

    protected abstract void writeCompressionHeader(final OutputStream os) throws IOException;
}
