package ru.vk.itmo.khadyrovalmasgali;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SSTable implements Comparable<SSTable> {

    private MemorySegment mappedData;
    private MemorySegment mappedIndexes;
    private long keysCount;
    private final long tableNum;
    private final Path dataPath;
    private final Path indexesPath;
    private final Path metaPath;
    public static final String SSTABLE_NAME_PREFIX = "ss";
    public static final String INDEX_NAME_PREFIX = "ind";
    public static final String META_NAME_PREFIX = "meta";

    public SSTable(Path path, String tableNum, Logger logger, Arena arena) {
        this.tableNum = Long.parseLong(tableNum);
        dataPath = path.resolve(SSTABLE_NAME_PREFIX + tableNum);
        indexesPath = path.resolve(INDEX_NAME_PREFIX + tableNum);
        metaPath = path.resolve(META_NAME_PREFIX + tableNum);
        try (FileChannel channel = FileChannel.open(
                dataPath,
                StandardOpenOption.READ)) {
            mappedData = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        } catch (IOException e) {
            logger.log(Level.ALL, e.getMessage());
        }
        try (FileChannel channel = FileChannel.open(
                indexesPath,
                StandardOpenOption.READ)) {
            mappedIndexes = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
        } catch (IOException e) {
            logger.log(Level.ALL, e.getMessage());
        }
        try (FileChannel channel = FileChannel.open(
                metaPath,
                StandardOpenOption.READ)) {
            MemorySegment mappedMeta = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size(), arena);
            keysCount = mappedMeta.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        } catch (IOException e) {
            logger.log(Level.ALL, e.getMessage());
        }
    }

    public Entry<MemorySegment> get(MemorySegment key) {
        long index = findIndex(key);
        return index >= 0 ? getByIndex(index) : null;
    }

    private BaseEntry<MemorySegment> getByIndex(long index) {
        long offset = mappedIndexes.get(ValueLayout.JAVA_LONG_UNALIGNED, index * Long.BYTES);
        long keySize = mappedData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        offset += Long.BYTES;
        MemorySegment mkey = mappedData.asSlice(offset, keySize);
        offset += keySize;
        long valueSize = mappedData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        if (valueSize == -1) {
            return new BaseEntry<>(mkey, null);
        }
        offset += Long.BYTES;
        MemorySegment mval = mappedData.asSlice(offset, valueSize);
        return new BaseEntry<>(mkey, mval);
    }

    private long findIndex(MemorySegment key) {
        long low = 0;
        long high = keysCount - 1;
        while (low <= high) {
            long mid = (low + high) >>> 1;
            long offset = mappedIndexes.get(ValueLayout.JAVA_LONG_UNALIGNED, mid * Long.BYTES);
            long keySize = mappedData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long mismatch = MemorySegment.mismatch(mappedData, offset, offset + keySize, key, 0, key.byteSize());
            if (mismatch == keySize) {
                low = mid + 1;
                continue;
            }
            if (mismatch == key.byteSize()) {
                high = mid - 1;
                continue;
            }
            if (mismatch == -1) {
                return mid;
            }

            int b1 = Byte.toUnsignedInt(mappedData.get(ValueLayout.JAVA_BYTE, offset + mismatch));
            int b2 = Byte.toUnsignedInt(key.get(ValueLayout.JAVA_BYTE, mismatch));
            if (b1 > b2) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return -(low + 1);
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new SSTableIterator(from, to);
    }

    private class SSTableIterator implements Iterator<Entry<MemorySegment>> {

        private long left;
        private long right;
        private Entry<MemorySegment> entry;

        public SSTableIterator(MemorySegment from, MemorySegment to) {
            left = 0;
            right = keysCount;
            if (from != null) {
                left = findIndex(from);
                if (left < 0) {
                    left = -(left + 1);
                }
            }
            if (to != null) {
                right = findIndex(to);
                if (right < 0) {
                    right = -(right + 1);
                }
            }
            getNextEntry();
        }

        @Override
        public boolean hasNext() {
            return entry != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            Entry<MemorySegment> result = entry;
            getNextEntry();
            return result;
        }

        private void getNextEntry() {
            if (left < right) {
                entry = getByIndex(left++);
            } else {
                entry = null;
            }
        }
    }

    @Override
    public int compareTo(SSTable o) {
        return Long.compare(o.tableNum, tableNum);
    }

    public void delete() throws IOException {
        Files.delete(dataPath);
        Files.delete(indexesPath);
        Files.delete(metaPath);
    }
}
