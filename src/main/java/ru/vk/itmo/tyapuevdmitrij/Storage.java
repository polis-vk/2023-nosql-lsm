package ru.vk.itmo.tyapuevdmitrij;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Storage {
    protected List<MemorySegment> ssTables;
    private long memTableEntriesSize;
    private static final String SS_TABLE_FILE_NAME = "ssTable";
    protected int ssTablesQuantity;

    public Storage(Path ssTablePath, Arena readArena) {
        ssTablesQuantity = findSsTablesQuantity(ssTablePath);
        ssTables = new ArrayList<>(ssTablesQuantity);
        if (ssTablesQuantity != 0) {
            for (int i = 0; i < ssTablesQuantity; i++) {
                Path path = ssTablePath.resolve(SS_TABLE_FILE_NAME + i);
                ssTables.add(getReadBufferFromSsTable(path, readArena));
            }
        }
    }

    private long getSsTableDataByteSize(Iterable<Entry<MemorySegment>> memTableEntries) {
        long ssTableDataByteSize = 0;
        long entriesCount = 0;
        for (Entry<MemorySegment> entry : memTableEntries) {
            ssTableDataByteSize += entry.key().byteSize();
            if (entry.value() != null) {
                ssTableDataByteSize += entry.value().byteSize();
            }
            entriesCount++;
        }
        memTableEntriesSize = entriesCount;
        return ssTableDataByteSize + entriesCount * Long.BYTES * 4L + Long.BYTES;
    }

    public void deleteOldSsTables(Path ssTablePath) throws NoSuchFieldException {
        File directory = new File(ssTablePath.toUri());
        File[] files = directory.listFiles();
        if (files == null) {
            throw new NoSuchFieldException();
        }
        for (File file : files) {
            if (!file.getName().contains(SS_TABLE_FILE_NAME + ssTablesQuantity)) {
                try {
                    Files.delete(file.toPath());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        boolean renamed;
        File remainingFile = files[0];
        String newFilePath = remainingFile.getParent() + File.separator + SS_TABLE_FILE_NAME + 0;
        renamed = remainingFile.renameTo(new File(newFilePath));
        if (!renamed) {
            throw new SecurityException();
        }
    }

    public void writeEntryAndIndexesToCompactionTable(MemorySegment buffer,
                                                      Entry<MemorySegment> entry, long[] offsets) {
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[1], offsets[0]);
        offsets[1] += Long.BYTES;
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[0], entry.key().byteSize());
        offsets[0] += Long.BYTES;
        MemorySegment.copy(entry.key(), 0, buffer, offsets[0], entry.key().byteSize());
        offsets[0] += entry.key().byteSize();
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[1], offsets[0]);
        offsets[1] += Long.BYTES;
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offsets[0], entry.value().byteSize());
        offsets[0] += Long.BYTES;
        MemorySegment.copy(entry.value(), 0, buffer, offsets[0], entry.value().byteSize());
        offsets[0] += entry.value().byteSize();
    }

    public MemorySegment getWriteBufferToSsTable(Long writeBytes, Path ssTablePath
    ) throws IOException {
        MemorySegment buffer;
        Path path = ssTablePath.resolve(SS_TABLE_FILE_NAME + ssTablesQuantity);
        Arena writeArena = Arena.ofConfined();
        try (FileChannel channel = FileChannel.open(path, EnumSet.of(StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING))) {
            buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0,
                    writeBytes, writeArena);
        }
        return buffer;
    }

    public void save(Iterable<Entry<MemorySegment>> memTableEntries, Path ssTablePath) throws IOException {
        MemorySegment buffer = getWriteBufferToSsTable(getSsTableDataByteSize(memTableEntries),
                ssTablePath);
        writeMemTableDataToFile(buffer, memTableEntries);
    }

    private void writeMemTableDataToFile(MemorySegment buffer, Iterable<Entry<MemorySegment>> memTableEntries) {
        long offset = 0;
        long bufferByteSize = buffer.byteSize();
        long writeIndexPosition = bufferByteSize - memTableEntriesSize * 2L * Long.BYTES - Long.BYTES;
        //write to the end of file size of memTable
        buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, bufferByteSize - Long.BYTES, memTableEntriesSize);
        for (Entry<MemorySegment> entry : memTableEntries) {
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.key().byteSize());
            //  write keyByteSizeOffsetPosition to the end of buffer
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, writeIndexPosition, offset);
            writeIndexPosition += Long.BYTES;
            offset += Long.BYTES;
            MemorySegment.copy(entry.key(), 0, buffer, offset, entry.key().byteSize());
            offset += entry.key().byteSize();
            //  write valueByteSizeOffsetPosition to the end of buffer next to the keyByteSizeOffsetPosition
            buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, writeIndexPosition, offset);
            writeIndexPosition += Long.BYTES;
            if (entry.value() == null) {
                buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, tombstone(offset));
                offset += Long.BYTES;
            } else {
                buffer.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entry.value().byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(entry.value(), 0, buffer, offset, entry.value().byteSize());
                offset += entry.value().byteSize();
            }
        }
    }

    private static MemorySegment getReadBufferFromSsTable(Path ssTablePath, Arena readArena) {
        MemorySegment buffer;
        boolean created = false;
        try (FileChannel channel = FileChannel.open(ssTablePath, StandardOpenOption.READ)) {
            buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, Files.size(ssTablePath), readArena);
            created = true;
        } catch (IOException e) {
            buffer = null;
        } finally {
            if (!created) {
                readArena.close();
            }
        }
        return buffer;
    }

    private long getSsTableIndexByKey(MemorySegment ssTable, MemorySegment key,
                                      Comparator<MemorySegment> memorySegmentComparator) {
        long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
        long left = 0;
        long right = memTableSize - 1L;
        long lastKeyIndexOffset = ssTable.byteSize() - 3 * Long.BYTES;
        while (left <= right) {
            long mid = (left + right) >>> 1;
            long midOffset = lastKeyIndexOffset - (memTableSize - 1L) * Long.BYTES * 2L + mid * 2L * Long.BYTES;
            MemorySegment readKey = getKeyByOffset(ssTable, midOffset);
            int res = memorySegmentComparator.compare(readKey, key);
            if (res == 0) {
                return mid;
            }
            if (res > 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return left;
    }

    private Entry<MemorySegment> getSsTableEntryByIndex(MemorySegment ssTable, long index) {
        long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
        if (index > memTableSize - 1 || index < 0) {
            return null;
        }
        long keyIndexOffset = ssTable.byteSize() - 3 * Long.BYTES
                - (memTableSize - 1L) * Long.BYTES * 2L + index * 2L * Long.BYTES;
        MemorySegment readKey = getKeyByOffset(ssTable, keyIndexOffset);
        MemorySegment readValue = getValueByOffset(ssTable, keyIndexOffset + Long.BYTES);
        return new BaseEntry<>(readKey, readValue);
    }

    private MemorySegment getKeyByOffset(MemorySegment ssTable, long offset) {
        long keyByteSizeOffset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long keyByteSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, keyByteSizeOffset);
        long keyOffset = keyByteSizeOffset + Long.BYTES;
        return ssTable.asSlice(keyOffset, keyByteSize);
    }

    private MemorySegment getValueByOffset(MemorySegment ssTable, long offset) {
        long valueByteSizeOffset = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long valueByteSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, valueByteSizeOffset);
        if (valueByteSize < 0) {
            return null;
        }
        long valueOffset = valueByteSizeOffset + Long.BYTES;
        return ssTable.asSlice(valueOffset, valueByteSize);
    }

    public Entry<MemorySegment> getSsTableDataByKey(MemorySegment key,
                                                    Comparator<MemorySegment> memorySegmentComparator) {
        for (int i = ssTables.size() - 1; i > -1; i--) {
            MemorySegment ssTable = ssTables.get(i);
            long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
            long left = 0;
            long right = memTableSize - 1L;
            long lastKeyIndexOffset = ssTable.byteSize() - 3 * Long.BYTES;
            while (left <= right) {
                long mid = (right + left) >>> 1;
                long midOffset = lastKeyIndexOffset - (memTableSize - 1L) * Long.BYTES * 2L + mid * 2L * Long.BYTES;
                MemorySegment readKey = getKeyByOffset(ssTable, midOffset);
                int res = memorySegmentComparator.compare(readKey, key);
                if (res == 0) {
                    MemorySegment value = getValueByOffset(ssTable, midOffset + Long.BYTES);
                    if (value == null) {
                        return null;
                    }
                    return new BaseEntry<>(key, value);
                } else if (res > 0) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
        }
        return null;
    }

    private int findSsTablesQuantity(Path ssTablePath) {
        File dir = new File(ssTablePath.toUri());
        File[] files = dir.listFiles();
        if (files == null) {
            return 0;
        }
        long countSsTables = Arrays.stream(files)
                .filter(file -> file.isFile() && file.getName().contains(SS_TABLE_FILE_NAME))
                .count();
        return (int) countSsTables;
    }

    public Iterator<Entry<MemorySegment>> range(
            Iterator<Entry<MemorySegment>> firstIterator,
            MemorySegment from,
            MemorySegment to, Comparator<MemorySegment> memorySegmentComparator) {
        List<Iterator<Entry<MemorySegment>>> iterators = new ArrayList<>(ssTablesQuantity + 1);
        for (MemorySegment memorySegment : ssTables) {
            iterators.add(iterator(memorySegment, from, to, memorySegmentComparator));
        }
        iterators.add(firstIterator);

        return new MergeIterator(iterators, Comparator.comparing(Entry::key, memorySegmentComparator));
    }

    private Iterator<Entry<MemorySegment>> iterator(MemorySegment ssTable, MemorySegment from, MemorySegment to,
                                                    Comparator<MemorySegment> memorySegmentComparator) {
        long recordIndexFrom = from == null ? 0 : normalize(getSsTableIndexByKey(ssTable,
                from,
                memorySegmentComparator));
        long memTableSize = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, ssTable.byteSize() - Long.BYTES);
        long recordIndexTo = to == null ? memTableSize : normalize(getSsTableIndexByKey(ssTable,
                to,
                memorySegmentComparator));

        return new Iterator<>() {
            long index = recordIndexFrom;

            @Override
            public boolean hasNext() {
                return index < recordIndexTo;
            }

            @Override
            public Entry<MemorySegment> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                Entry<MemorySegment> entry = getSsTableEntryByIndex(ssTable, index);
                index++;
                return entry;
            }
        };
    }

    private static long tombstone(long offset) {
        return 1L << 63 | offset;
    }

    private static long normalize(long value) {
        return value & ~(1L << 63);
    }

}
