package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final Comparator<MemorySegment> comparator = new MSComparator();
    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memTableMap =
            new ConcurrentSkipListMap<>(comparator);

    private final Arena readArena = Arena.ofShared();
    private final NavigableMap<Long, MemorySegment> readMappedMemorySegments = new TreeMap<>(); // SSTables
    private final Path path;
    private final Path ssTablePath;
    private final long latestFileIndex;

    public InMemoryDao(Config config) {
        this.path = config.basePath();
        this.ssTablePath = path.resolve(Constants.FILE_NAME_PEFIX);

        latestFileIndex = readConfig();
    }

    private long readConfig() {
        Path configPath = path.resolve(Constants.FILE_NAME_CONFIG);
        try (Arena readConfigArena = Arena.ofConfined()) {
            MemorySegment configMemorySegment = FileChannel.open(configPath, Constants.READ_OPTIONS)
                    .map(FileChannel.MapMode.READ_ONLY, 0, Files.size(configPath), readConfigArena);
            return configMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        // First trying to return the file from MemTable
        Entry<MemorySegment> entry = memTableMap.get(key);
        if (entry != null) {
            return filterNullValue(entry);
        }

        for (long i = latestFileIndex; i >= 0; i--) {
            // Pull files into memory and save them so as not to pull them in again
            var readMappedMemorySegment = readMappedMemorySegments.getOrDefault(i, readFileAtIndex(i));
            if (readMappedMemorySegment == null) {
                continue;
            }

            // Search for each file, pulling out a page from memory
            Entry<MemorySegment> entryFromFile = binarySearchSSTable(readMappedMemorySegment, key);
            if (entryFromFile != null) {
                return filterNullValue(entryFromFile);
            }
        }

        return null;
    }

    private Entry<MemorySegment> binarySearchSSTable(MemorySegment sstable, MemorySegment key) {
        boolean needMoreBites;
        long offset = 0L;
        long biteCount = 0L;
        List<Long> offsets = new ArrayList<>();
        while (offset < sstable.byteSize()) {
            biteCount++;
            offsets.clear();

            // Each time we pull out a page until we reach the end
            needMoreBites = bite(sstable, key, offset, biteCount, offsets);

            // Binary search
            Entry<MemorySegment> entryFromFile = binarySearch(offsets, key, sstable);
            if (entryFromFile != null) {
                return entryFromFile;
            }
            if (!needMoreBites) break;
        }

        return null;
    }

    private boolean bite(MemorySegment sStable, MemorySegment key, Long offset, Long biteCount, List<Long> offsets) {
        while (offset < Constants.PAGE_SIZE * biteCount && offset < sStable.byteSize()) {
            long keySize = sStable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            long valueSize = sStable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + keySize);

            long currentTotalSize;
            if (valueSize == -1L) {
                currentTotalSize = offset + keySize + Long.BYTES + Long.BYTES;
            } else {
                currentTotalSize = offset + keySize + Long.BYTES + valueSize;
            }
            if (currentTotalSize > Constants.PAGE_SIZE * biteCount) {
                offset -= Long.BYTES;
                break;
            } else {
                if (keySize == key.byteSize()) {
                    // Will search only by keys equal in length
                    offsets.add(offset - Long.BYTES);
                }
                if (keySize > key.byteSize()) {
                    return false;
                }
                if (valueSize == -1L) {
                    offset += keySize + Long.BYTES + Long.BYTES;
                } else {
                    offset += keySize + Long.BYTES + valueSize;
                }
            }
        }

        return true;
    }

    private MemorySegment readFileAtIndex(long index) {
        Path ssTableNumPath = Paths.get(ssTablePath + Long.toString(index));
        MemorySegment tryReadMappedMemorySegment;
        try {
            tryReadMappedMemorySegment = FileChannel.open(ssTableNumPath, Constants.READ_OPTIONS)
                    .map(FileChannel.MapMode.READ_ONLY, 0, Files.size(ssTableNumPath), readArena);
        } catch (IOException e) {
            tryReadMappedMemorySegment = null;
        }
        return readMappedMemorySegments.put(index, tryReadMappedMemorySegment);
    }

    private Entry<MemorySegment> filterNullValue(Entry<MemorySegment> entry) {
        if (entry.value() == null) {
            return null;
        }

        return entry;
    }

    private Entry<MemorySegment> binarySearch(List<Long> offsets, MemorySegment key, MemorySegment memorySegment) {
        int left = 0;
        int right = offsets.size() - 1;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            long offset = offsets.get(mid);
            long keySize = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            offset += Long.BYTES;
            MemorySegment midKey = memorySegment.asSlice(offset, keySize);

            int compareResult = comparator.compare(key, midKey);
            if (compareResult == 0) {
                offset += keySize;
                long valueSize = memorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment midValue;
                if (valueSize == -1L) {
                    midValue = null;
                } else {
                    midValue = memorySegment.asSlice(offset, valueSize);
                }
                return new BaseEntry<>(midKey, midValue);
            } else if (compareResult > 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return null;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        // Read all missing files
        for (long i = latestFileIndex; i >= 0; i--) {
            readMappedMemorySegments.getOrDefault(i, readFileAtIndex(i));
        }

        return new MemFileIterator(comparator, readMappedMemorySegments, memTableMap, from, to);
    }

    @Override
    public void close() throws IOException {
        // Freeing the arena to open a writing channel
        if (!readArena.scope().isAlive()) {
            return;
        }
        readArena.close();
        Arena writeArena = Arena.ofConfined();

        // Calculate the writing size, using all the entries and their sizes
        long mappedMemorySize =
                memTableMap.values().stream().mapToLong(e -> {
                    long valueSize;
                    if (e.value() == null) {
                        valueSize = 0L;
                    } else {
                        valueSize = e.value().byteSize();
                    }
                    return e.key().byteSize() + valueSize;
                }).sum();
        mappedMemorySize += Long.BYTES * memTableMap.size() * 2L;

        // Memory segment to write
        Path ssTableNumPath = Paths.get(ssTablePath + Long.toString(latestFileIndex + 1));
        MemorySegment writeMappedMemorySegment = FileChannel.open(ssTableNumPath, Constants.WRITE_OPTIONS)
                .map(FileChannel.MapMode.READ_WRITE, 0, mappedMemorySize, writeArena);

        // Write memTable
        writeMemTableToSSTable(writeMappedMemorySegment);
        writeArena.close();

        // Update config
        Arena writeConfigArena = Arena.ofConfined();
        MemorySegment writeConfig = FileChannel.open(path.resolve(Constants.FILE_NAME_CONFIG), Constants.WRITE_OPTIONS)
                .map(FileChannel.MapMode.READ_WRITE, 0, Long.BYTES, writeConfigArena);
        writeConfig.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, latestFileIndex + 1);
        writeConfigArena.close();
    }

    private void writeMemTableToSSTable(MemorySegment writeMappedMemorySegment) {
        long offset = 0;
        for (Entry<MemorySegment> entry : memTableMap.values()) {
            MemorySegment key = entry.key();
            writeMappedMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, key.byteSize());
            offset += Long.BYTES;
            MemorySegment.copy(key, 0, writeMappedMemorySegment, offset, key.byteSize());
            offset += key.byteSize();

            MemorySegment value = entry.value();
            if (value == null) {
                writeMappedMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, -1L);
                offset += Long.BYTES;
            } else {
                writeMappedMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value.byteSize());
                offset += Long.BYTES;
                MemorySegment.copy(value, 0, writeMappedMemorySegment, offset, value.byteSize());
                offset += value.byteSize();
            }
        }
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTableMap.put(entry.key(), entry);
    }

    @Override
    public Iterator<Entry<MemorySegment>> all() {
        return memTableMap.values().iterator();
    }
}
