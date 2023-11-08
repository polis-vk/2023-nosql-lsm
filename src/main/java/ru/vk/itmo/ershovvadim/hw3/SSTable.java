package ru.vk.itmo.ershovvadim.hw3;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.nio.file.StandardOpenOption.READ;

public class SSTable {

    private final int priority;
    private final MemorySegment mapIndex;
    private final MemorySegment mapData;

    public SSTable(Path dir, Arena arena) throws IOException {
        this.priority = Integer.parseInt(
                dir.getFileName().toString().substring(PersistenceManyFilesDao.SS_TABLE_DIR.length())
        );

        Path dataFile = dir.resolve(PersistenceManyFilesDao.DATA);
        Path indexFile = dir.resolve(PersistenceManyFilesDao.INDEX);
        try (var fcFile = FileChannel.open(dataFile, READ);
             var fcIndex = FileChannel.open(indexFile, READ)
        ) {
            this.mapData = fcFile.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(dataFile), arena);
            this.mapIndex = fcIndex.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(indexFile), arena);
        }
    }

    public PeekIterator<Entry<MemorySegment>> iterator(MemorySegment from, MemorySegment to) {
        return new PeekIteratorImpl(new Itr(from, to), priority);
    }

    private class Itr implements Iterator<Entry<MemorySegment>> {

        private final MemorySegment to;

        private long indexOffset;

        private long currentKeyOffset = -1;
        private long currentKeySize = -1;

        Itr(MemorySegment from, MemorySegment to) {
            if (from == null) {
                this.indexOffset = 0;
            } else {
                this.indexOffset = binarySearch(from);
            }
            this.to = to;
        }

        @Override
        public boolean hasNext() {
            if (indexOffset == SSTable.this.mapIndex.byteSize()) {
                return false;
            }
            if (to == null) {
                return true;
            }
            this.currentKeyOffset = SSTable.this.mapIndex.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
            this.currentKeySize = SSTable.this.mapData.get(ValueLayout.JAVA_LONG_UNALIGNED, currentKeyOffset);

            long fromOffset = currentKeyOffset + Long.BYTES;
            return Utils.compare(to, SSTable.this.mapData, fromOffset, fromOffset + currentKeySize) > 0;
        }

        @Override
        public Entry<MemorySegment> next() throws NoSuchElementException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            long keyOffset;
            long keySize;
            if (currentKeyOffset == -1 || currentKeySize == -1) {
                keyOffset = SSTable.this.mapIndex.get(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset);
                keySize = SSTable.this.mapData.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
            } else {
                keyOffset = currentKeyOffset;
                keySize = currentKeySize;
            }
            indexOffset += Long.BYTES;
            keyOffset += Long.BYTES;
            MemorySegment key = SSTable.this.mapData.asSlice(keyOffset, keySize);
            keyOffset += keySize;

            long valueSize = SSTable.this.mapData.get(ValueLayout.JAVA_LONG_UNALIGNED, keyOffset);
            MemorySegment value;
            if (valueSize == -1) {
                value = null;
            } else {
                value = SSTable.this.mapData.asSlice(keyOffset + Long.BYTES, valueSize);
            }

            return new BaseEntry<>(key, value);
        }

        private long binarySearch(MemorySegment key) {
            long low = 0;
            long high = SSTable.this.mapIndex.byteSize() / Long.BYTES - 1;

            while (low <= high) {
                long mid = low + ((high - low) / 2);

                long iterOffset = SSTable.this.mapIndex.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);

                long msKeySize = SSTable.this.mapData.get(ValueLayout.JAVA_LONG_UNALIGNED, iterOffset);
                iterOffset += Long.BYTES;

                int comparator = Utils.compare(key, SSTable.this.mapData, iterOffset, iterOffset + msKeySize);
                if (comparator > 0) {
                    low = mid + 1;
                } else if (comparator < 0) {
                    high = mid - 1;
                } else {
                    return mid * Long.BYTES;
                }
            }
            return low * Long.BYTES;
        }
    }
}
