package ru.vk.itmo.viktorkorotkikh.io.read;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.viktorkorotkikh.LSMPointerIterator;
import ru.vk.itmo.viktorkorotkikh.MemorySegmentComparator;
import ru.vk.itmo.viktorkorotkikh.Utils;
import ru.vk.itmo.viktorkorotkikh.decompressor.Decompressor;
import ru.vk.itmo.viktorkorotkikh.decompressor.LZ4Decompressor;
import ru.vk.itmo.viktorkorotkikh.exceptions.UnknownCompressorTypeException;
import ru.vk.itmo.viktorkorotkikh.io.ByteArraySegment;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * <B>compression info</B>:
 * isCompressed|algorithm|blocksCount|uncompressedBlockSize|block1Offset|block2Offset|blockNOffset
 * <p/>
 * <B>index</B>:
 * hasNoTombstones|entriesSize|key1BlockNumber|key1SizeBlockOffset|key2BlockNumber|key2SizeBlockOffset|keyNBlockNumber|keyNSizeBlockOffset|
 * <p/>
 * keyNBlockNumber - номер блока для начала ключа номер N (key1Size|key1|value1Size|value1)
 * <br/>
 * keyNSizeBlockOffset - смещение начала размера ключа внутри блока
 * <p/>
 * <B>blocks</B>:
 * block1|block2|...|blockN
 */
public class CompressedSSTableReader extends AbstractSSTableReader {
    private final int uncompressedBlockSize;
    private final int blocksCount;
    private final Decompressor decompressor;

    // hasNoTombstones|entriesSize|key1BlockNumber|key1SizeBlockOffset|...|keyNBlockNumber|keyNSizeBlockOffset|
    private static final long INDEX_FILE_METADATA_OFFSET = 1L + Long.BYTES;
    private static final long KEY_SIZE_BLOCK_OFFSET = 2L * Integer.BYTES;

    private static final ScopedValue<ByteArraySegment> blockBuffer = ScopedValue.newInstance();
    private static final ScopedValue<ByteArraySegment> uncompressedBlockBuffer = ScopedValue.newInstance();

    private static final class LastUncompressedBlockInfo {
        int lastUncompressedBlockNumber = -1;
        int lastUncompressedBlockOffset = -1;
    }

    private static final ScopedValue<LastUncompressedBlockInfo> lastUncompressedBlockInfo = ScopedValue.newInstance();

    public CompressedSSTableReader(
            MemorySegment mappedSSTable,
            MemorySegment mappedIndexFile,
            MemorySegment mappedCompressionInfo,
            Decompressor decompressor,
            int index
    ) {
        super(mappedSSTable, mappedIndexFile, mappedCompressionInfo, index);
        // isCompressed|algorithm|blocksCount|uncompressedBlockSize
        //     1b      |    1b   |     4b    |        4b
        this.uncompressedBlockSize = mappedCompressionInfo.get(ValueLayout.JAVA_INT_UNALIGNED, 2L + Integer.BYTES);
        this.blocksCount = mappedCompressionInfo.get(ValueLayout.JAVA_INT_UNALIGNED, 2L);

        this.decompressor = decompressor;
    }

    @Override
    public LSMPointerIterator iterator(MemorySegment from, MemorySegment to) throws Exception {
        int fromPosition = 0;
        int toPosition = (int) getEntriesSize() - 1;
        ByteArraySegment iteratorBlockBuffer = null;
        ByteArraySegment iteratorUncompressedBuffer = null;
        LastUncompressedBlockInfo iteratorLastUncompressedBlockInfo = null;
        if (from != null) {
            // optimization - if the entire record is in a block,
            // then the first time we call next we don’t need to read and decompress the data again
            iteratorBlockBuffer = getBuffer();
            iteratorUncompressedBuffer = getBuffer();
            iteratorLastUncompressedBlockInfo = new LastUncompressedBlockInfo();
            fromPosition = ScopedValue
                    .where(blockBuffer, iteratorBlockBuffer)
                    .where(uncompressedBlockBuffer, iteratorUncompressedBuffer)
                    .where(lastUncompressedBlockInfo, iteratorLastUncompressedBlockInfo)
                    .call(() -> (int) getEntryOffset(from, SearchOption.GTE));
            if (fromPosition == -1) {
                return new CompressedSSTableIterator(0, -1, null, null, null);
            }
        }
        if (to != null) {
            ByteArraySegment toBlockBuffer = getBuffer();
            ByteArraySegment toUncompressedBuffer = getBuffer();
            LastUncompressedBlockInfo toLastUncompressedBlockInfo = new LastUncompressedBlockInfo();
            toPosition = ScopedValue
                    .where(blockBuffer, toBlockBuffer)
                    .where(uncompressedBlockBuffer, toUncompressedBuffer)
                    .where(lastUncompressedBlockInfo, toLastUncompressedBlockInfo)
                    .call(() -> (int) getEntryOffset(to, SearchOption.LT));
            if (toPosition == -1) {
                return new CompressedSSTableIterator(0, -1, null, null, null);
            }
        }
        if (from == null) {
            iteratorBlockBuffer = getBuffer();
            iteratorUncompressedBuffer = getBuffer();
            iteratorLastUncompressedBlockInfo = new LastUncompressedBlockInfo();
        }

        return new CompressedSSTableIterator(
                fromPosition,
                toPosition,
                iteratorBlockBuffer,
                iteratorUncompressedBuffer,
                iteratorLastUncompressedBlockInfo
        );
    }

    private ByteArraySegment getBuffer() {
        return new ByteArraySegment(uncompressedBlockSize); // uncompressedBlockSize is maxSize of compressed data
    }

    private int getBlockOffset(int blockNumber) {
        return mappedCompressionInfo.get(
                ValueLayout.JAVA_INT_UNALIGNED,
                // isCompressed|algorithm|blocksCount|uncompressedBlockSize|blockNOffset
                (1L + 1L + Integer.BYTES + Integer.BYTES + (long) blockNumber * Integer.BYTES)
        );
    }

    private void decompressOneBlock(
            ByteArraySegment blockBuffer,
            long srcOffset,
            byte[] dest,
            int destOff,
            int compressedSize,
            int uncompressedSize
    ) throws IOException {
        MemorySegment.copy(
                mappedSSTable,
                srcOffset,
                blockBuffer.segment(),
                0L,
                compressedSize
        );
        decompressor.decompress(blockBuffer.getArray(), dest, destOff, uncompressedSize);
    }

    @Override
    protected Entry<MemorySegment> getByIndex(long index) throws IOException {
        return readEntry((int) index, true);
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        try {
            return ScopedValue
                    .where(blockBuffer, getBuffer())
                    .where(uncompressedBlockBuffer, getBuffer())
                    .where(lastUncompressedBlockInfo, new LastUncompressedBlockInfo())
                    .call(() -> super.get(key));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Entry<MemorySegment> readEntry(int index, boolean readValue) throws IOException {
        int keyNBlockNumber = mappedIndexFile.get(
                ValueLayout.JAVA_INT_UNALIGNED,
                INDEX_FILE_METADATA_OFFSET + KEY_SIZE_BLOCK_OFFSET * index
        );
        int keyNSizeBlockOffset = mappedIndexFile.get(
                ValueLayout.JAVA_INT_UNALIGNED,
                INDEX_FILE_METADATA_OFFSET + KEY_SIZE_BLOCK_OFFSET * index + Integer.BYTES
        );

        byte[] decompressedKeySize = new byte[Long.BYTES];
        readCompressed(keyNBlockNumber, blockBuffer.get(), decompressedKeySize, keyNSizeBlockOffset);

        long keySize = byteArrayToLong(decompressedKeySize, 0);

        ByteArraySegment keyByteArray = new ByteArraySegment((int) keySize);
        readCompressed(
                lastUncompressedBlockInfo.get().lastUncompressedBlockNumber,
                blockBuffer.get(),
                keyByteArray.getArray(),
                lastUncompressedBlockInfo.get().lastUncompressedBlockOffset
        );

        if (!readValue) {
            return new BaseEntry<>(keyByteArray.segment(), null);
        }

        byte[] decompressedValueSize = new byte[Long.BYTES];
        readCompressed(
                lastUncompressedBlockInfo.get().lastUncompressedBlockNumber,
                blockBuffer.get(),
                decompressedValueSize,
                lastUncompressedBlockInfo.get().lastUncompressedBlockOffset
        );
        long valueSize = byteArrayToLong(decompressedValueSize, 0);

        if (valueSize == -1) {
            return new BaseEntry<>(keyByteArray.segment(), null);
        } else { // read value
            ByteArraySegment valueByteArray = new ByteArraySegment((int) keySize);
            readCompressed(
                    lastUncompressedBlockInfo.get().lastUncompressedBlockNumber,
                    blockBuffer.get(),
                    valueByteArray.getArray(),
                    lastUncompressedBlockInfo.get().lastUncompressedBlockOffset
            );
            return new BaseEntry<>(keyByteArray.segment(), valueByteArray.segment());
        }

        /*int tailOfLastBlock = decompressedKeySize.length - (keyNSizeBlockOffset + Long.BYTES);
        ByteArraySegment decompressedKey;
        MemorySegment key;

        if (tailOfLastBlock < keySize) { // need to read more blocks
            int countOfBlocksToReadKey = (int) ((keySize - tailOfLastBlock) / uncompressedBlockSize + 1);
            // allocate more space than keySize because we read entire block
            decompressedKey
                    = new ByteArraySegment(tailOfLastBlock + countOfBlocksToReadKey * uncompressedBlockSize);

            // copy tail of last decompressed block
            System.arraycopy(
                    decompressedKeySize,
                    (keyNSizeBlockOffset + Long.BYTES),
                    decompressedKey.getArray(),
                    0,
                    tailOfLastBlock
            );

            keyNBlockNumber = decompressBlocks(
                    keyNBlockNumber,
                    blockBuffer.get(),
                    decompressedKey.getArray(),
                    countOfBlocksToReadKey
            );
            key = decompressedKey.segment().asSlice(0, keySize);
        } else { // last read block contains key
            // copy key
            decompressedKey = new ByteArraySegment((int) keySize);
            System.arraycopy(
                    decompressedKeySize,
                    (keyNSizeBlockOffset + Long.BYTES),
                    decompressedKey.getArray(),
                    0,
                    (int) keySize
            );
            key = decompressedKey.segment();
        }
        if (!readValue) {
            return new BaseEntry<>(key, null);
        }

        int tailOfLastReadKeyBlock = (int) (tailOfLastBlock - keySize);

        long valueSize;
        byte[] decompressedValueSize;

        if (tailOfLastReadKeyBlock < Long.BYTES) { // need to read more blocks
            int countOfBlocksToReadValueSize = (Long.BYTES - tailOfLastReadKeyBlock) / uncompressedBlockSize + 1;
            // allocate more space than keySize because we read entire block
            decompressedValueSize
                    = new byte[tailOfLastReadKeyBlock + countOfBlocksToReadValueSize * uncompressedBlockSize];

            // copy tail of last decompressed block
            System.arraycopy(
                    decompressedKey.getArray(),
                    (int) keySize,
                    decompressedValueSize,
                    0,
                    tailOfLastReadKeyBlock
            );

            keyNBlockNumber = decompressBlocks(
                    keyNBlockNumber,
                    blockBuffer.get(),
                    decompressedValueSize,
                    countOfBlocksToReadValueSize
            );
        } else {
            // copy value size
            decompressedValueSize = new byte[Long.BYTES];
            System.arraycopy(
                    decompressedKeySize,
                    (int) (keyNSizeBlockOffset + Long.BYTES + keySize),
                    decompressedValueSize,
                    0,
                    Long.BYTES
            );
        }
        valueSize = byteArrayToLong(decompressedValueSize, 0);
        MemorySegment value;

        if (valueSize == -1) {
            value = null;
        } else { // read value
            int tailOfLastReadValueSizeBlock = (tailOfLastReadKeyBlock - Long.BYTES);
            if (tailOfLastReadValueSizeBlock < valueSize) { // need to read more blocks
                int countOfBlocksToReadValue
                        = (int) ((valueSize - tailOfLastReadValueSizeBlock) / uncompressedBlockSize + 1);
                // allocate more space than keySize because we read entire block
                ByteArraySegment decompressedValue = new ByteArraySegment(
                        tailOfLastReadValueSizeBlock + countOfBlocksToReadValue * uncompressedBlockSize
                );

                // copy tail of last decompressed block
                System.arraycopy(
                        decompressedValueSize,
                        Long.BYTES,
                        decompressedValue.getArray(),
                        0,
                        tailOfLastReadValueSizeBlock
                );

                keyNBlockNumber = decompressBlocks(
                        keyNBlockNumber,
                        blockBuffer.get(),
                        decompressedValue.getArray(),
                        countOfBlocksToReadValue
                );
                value = decompressedValue.segment().asSlice(0, valueSize);
            } else { // last read block contains value
                // copy value
                ByteArraySegment decompressedValue = new ByteArraySegment((int) valueSize);
                System.arraycopy(
                        decompressedValueSize,
                        Long.BYTES,
                        decompressedValue.getArray(),
                        0,
                        (int) valueSize
                );
                value = decompressedValue.segment();
            }
        }

        return new DecompressedEntry(key, value, keyNBlockNumber - initialKeyNBlockNumber);*/
    }

    private int decompressBlocks(
            int startBlockNumber,
            ByteArraySegment blockBuffer,
            byte[] targetDecompressedByteArray,
            int countOfBlocksToRead
    ) throws IOException {
        return decompressBlocks(
                startBlockNumber,
                startBlockNumber,
                blockBuffer,
                targetDecompressedByteArray,
                countOfBlocksToRead
        );
    }

    private int decompressBlocks(
            int start,
            int startBlockNumber,
            ByteArraySegment blockBuffer,
            byte[] targetDecompressedByteArray,
            int countOfBlocksToRead
    ) throws IOException {
        for (int i = start; i < countOfBlocksToRead; i++) {
            int blockStart = getBlockOffset(startBlockNumber);
            int blockEnd = startBlockNumber + 1 == blocksCount
                    ? (int) mappedCompressionInfo.byteSize()
                    : getBlockOffset(startBlockNumber + 1);

            int compressedSize = blockEnd - blockStart;
            decompressOneBlock(
                    blockBuffer,
                    blockStart,
                    uncompressedBlockBuffer.get().getArray(),
                    0,
                    compressedSize,
                    uncompressedBlockSize
            );
            // ====================================================================
            //               BLOCK
            // |................................|
            //  ^_____DATA_____^
            // countOfBlocksToRead - start == 1, so we should copy only DATA.length
            // ====================================================================
            //               BLOCK                              BLOCK
            // |................................||................................|
            //                            ^_____DATA_____^
            // countOfBlocksToRead - start > 1, so we should copy
            // ====================================================================
            System.arraycopy(
                    uncompressedBlockBuffer.get().getArray(),
                    0,
                    targetDecompressedByteArray,
                    i * uncompressedBlockSize,
                    Math.min(// fill full tail of targetDecompressedByteArray
                            targetDecompressedByteArray.length - i * uncompressedBlockSize,
                            // or
                            (uncompressedBlockSize - 1) * i
                    )
            );

            startBlockNumber++;
        }
        return startBlockNumber;
    }

    private int readCompressed(
            int startBlockNumber,
            ByteArraySegment blockBuffer,
            byte[] targetDecompressedByteArray,
            int blockOffset
    ) throws IOException {
        // ====================================================================
        //               BLOCK
        // |................................|
        //  ^_____DATA_____^
        // uncompressedBlockSize - blockOffset >= targetDecompressedByteArray.length
        // so we should copy only DATA.length
        // ====================================================================
        //               BLOCK                              BLOCK
        // |................................||................................|
        //                            ^_____DATA_____^
        // uncompressedBlockSize - blockOffset < targetDecompressedByteArray.length
        // so we should copy tail of startBlockNumber and part of the next block(s)
        // ====================================================================

        int decompressedAndCopiedLength = 0;
        LastUncompressedBlockInfo localLastUncompressedBlockInfo = lastUncompressedBlockInfo.get();
        while (decompressedAndCopiedLength < targetDecompressedByteArray.length) {
            if (localLastUncompressedBlockInfo.lastUncompressedBlockNumber == startBlockNumber &&
                    localLastUncompressedBlockInfo.lastUncompressedBlockOffset < uncompressedBlockSize) {
                // read tail from the last uncompressed block
                int length = readFromDecompressedBlock(
                        targetDecompressedByteArray,
                        decompressedAndCopiedLength,
                        blockOffset
                );
                decompressedAndCopiedLength += length;
                localLastUncompressedBlockInfo.lastUncompressedBlockOffset = blockOffset + length;
                if (localLastUncompressedBlockInfo.lastUncompressedBlockOffset >= uncompressedBlockSize) {
                    blockOffset = 0;
                    startBlockNumber++;
                }
                continue;
            }
            int blockStart = getBlockOffset(startBlockNumber);
            int blockEnd = startBlockNumber + 1 >= blocksCount
                    ? (int) mappedSSTable.byteSize()
                    : getBlockOffset(startBlockNumber + 1);
            int compressedSize = blockEnd - blockStart;
            // todo workaround not full last block
            int uncompressedSize = startBlockNumber + 1 == blocksCount
                    ? getLastDataUncompressedSize()
                    : uncompressedBlockSize;
            decompressOneBlock(
                    blockBuffer,
                    blockStart,
                    uncompressedBlockBuffer.get().getArray(),
                    0,
                    compressedSize,
                    uncompressedSize
            );
            int length = readFromDecompressedBlock(
                    targetDecompressedByteArray,
                    decompressedAndCopiedLength,
                    blockOffset
            );
            decompressedAndCopiedLength += length;
            blockOffset += length;

            localLastUncompressedBlockInfo.lastUncompressedBlockNumber = startBlockNumber;
            localLastUncompressedBlockInfo.lastUncompressedBlockOffset = blockOffset;
            if (localLastUncompressedBlockInfo.lastUncompressedBlockOffset >= uncompressedBlockSize) {
                blockOffset = 0;
            }

            startBlockNumber++;
        }

        return startBlockNumber;
    }

    private int getLastDataUncompressedSize() {
        return mappedCompressionInfo.get(
                ValueLayout.JAVA_INT_UNALIGNED,
                mappedCompressionInfo.byteSize() - Integer.BYTES
        );
    }

    /**
     * @param target       target array
     * @param targetOffset starting position in the target data
     * @param blockOffset  offset in uncompressedBlockBuffer
     * @return the total number of bytes read into the buffer
     */
    private int readFromDecompressedBlock(byte[] target, int targetOffset, int blockOffset) {
        int length = Math.min(
                target.length - targetOffset,
                uncompressedBlockSize - blockOffset
        );

        System.arraycopy(
                uncompressedBlockBuffer.get().getArray(),
                blockOffset,
                target,
                targetOffset,
                length
        );
        return length;
    }

    private static long byteArrayToLong(byte[] bytes, int offset) {
        long value = 0;
        for (int i = offset + Long.BYTES - 1; i >= offset; i--) {
            value <<= 8;
            value |= (bytes[i] & 0xFFL);
        }
        return value;
    }

    private long getEntriesSize() {
        return mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, 1);
    }

    @Override
    protected long getEntryOffset(MemorySegment key, SearchOption searchOption) throws IOException {
        // binary search
        int left = 0;
        int right = (int) (getEntriesSize() - 1);
        while (left <= right) {
            int mid = (right + left) >>> 1;
            MemorySegment decompressedKey = readEntry(mid, false).key();

            int keyComparison = MemorySegmentComparator.INSTANCE.compare(
                    decompressedKey,
                    0L,
                    decompressedKey.byteSize(),
                    key
            );

            if (keyComparison < 0) {
                left = mid + 1;
            } else if (keyComparison > 0) {
                right = mid - 1;
            } else {
                return switch (searchOption) {
                    case EQ, GTE -> mid;
                    case LT -> mid - 1;
                };
            }
        }

        return switch (searchOption) {
            case EQ -> -1;
            case GTE -> {
                if (left == blocksCount) {
                    yield -1;
                } else {
                    yield left;
                }
            }
            case LT -> right;
        };
    }

    public final class CompressedSSTableIterator extends LSMPointerIterator {
        private int startEntityNumber;
        private final int endEntityNumber;
        private Entry<MemorySegment> current;

        private final ByteArraySegment iteratorCompressedBlockBuffer;
        private final ByteArraySegment iteratorUncompressedBlockBuffer;
        private final LastUncompressedBlockInfo iteratorLastUncompressedBlockInfo;

        private CompressedSSTableIterator(
                int startEntityNumber,
                int endEntityNumber,
                ByteArraySegment iteratorCompressedBlockBuffer,
                ByteArraySegment iteratorUncompressedBlockBuffer,
                LastUncompressedBlockInfo iteratorLastUncompressedBlockInfo
        ) throws Exception {
            this.startEntityNumber = startEntityNumber;
            this.endEntityNumber = endEntityNumber;
            this.iteratorCompressedBlockBuffer = iteratorCompressedBlockBuffer;
            this.iteratorUncompressedBlockBuffer = iteratorUncompressedBlockBuffer;
            this.iteratorLastUncompressedBlockInfo = iteratorLastUncompressedBlockInfo;
            if (startEntityNumber <= endEntityNumber) {
                current = ScopedValue
                        .where(blockBuffer, this.iteratorCompressedBlockBuffer)
                        .where(uncompressedBlockBuffer, this.iteratorUncompressedBlockBuffer)
                        .where(lastUncompressedBlockInfo, this.iteratorLastUncompressedBlockInfo)
                        .call(() -> {
                            try {
                                return readEntry(startEntityNumber, true);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                this.startEntityNumber += 1;
            }
        }

        @Override
        public int getPriority() {
            return index;
        }

        @Override
        protected MemorySegment getPointerKeySrc() {
            return current.key();
        }

        @Override
        protected long getPointerKeySrcOffset() {
            return 0;
        }

        @Override
        protected long getPointerKeySrcSize() {
            return current.key().byteSize();
        }

        @Override
        public boolean isPointerOnTombstone() {
            return current.value() == null;
        }

        @Override
        public void shift() {
            int finalStartBlockNumber = startEntityNumber;
            try {
                current = ScopedValue
                        .where(blockBuffer, this.iteratorCompressedBlockBuffer)
                        .where(uncompressedBlockBuffer, this.iteratorUncompressedBlockBuffer)
                        .where(lastUncompressedBlockInfo, this.iteratorLastUncompressedBlockInfo)
                        .call(() -> {
                            try {
                                return readEntry(finalStartBlockNumber, true);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                startEntityNumber += 1;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public long getPointerSize() {
            return Utils.getEntrySize(current);
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            Entry<MemorySegment> next = new BaseEntry<>(current.key(), current.value());
            if (startEntityNumber > endEntityNumber) {
                current = null;
            } else {
                shift();
            }
            return next;
        }
    }

    public static boolean isCompressed(MemorySegment mappedCompressionInfo) {
        return mappedCompressionInfo.get(ValueLayout.JAVA_BOOLEAN, 0);
    }

    public static Decompressor getDecompressor(MemorySegment mappedCompressionInfo) {
        byte compressorType = mappedCompressionInfo.get(ValueLayout.JAVA_BYTE, 1);
        return switch (compressorType) {
            case 0 -> LZ4Decompressor.INSTANCE;
            default -> throw new UnknownCompressorTypeException(compressorType);
        };
    }
}
