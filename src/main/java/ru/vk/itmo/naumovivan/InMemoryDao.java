package ru.vk.itmo.naumovivan;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    final static private String FN_INDEX_SUFFIX = "idx";
    final static private String FN_DATA_SUFFIX = "data";

    private final NavigableMap<MemorySegment, Entry<MemorySegment>> memtable =
            new ConcurrentSkipListMap<>(DaoUtils::compareMemorySegments);
    private final Path basePath;
    private final Arena arena;
    private final List<MemorySegment> dataPages;
    private final List<MemorySegment> indexPages;

    /**
     * Maps SSTable to {@link MemorySegment}s and stores them in {@code dataPages} and {@code indexPages} lists.
     *
     * @param fn SSTable file name without extension
     * @throws IOException if an I/O error occurs
     */
    private void mapSSTable(final String fn) throws IOException {
        final Path dataPath = basePath.resolve(fn + "." + FN_DATA_SUFFIX);
        final Path indexPath = basePath.resolve(fn + "." + FN_INDEX_SUFFIX);
        long dataSize;
        long indexSize;
        try {
            dataSize = Files.size(dataPath);
            indexSize = Files.size(indexPath);
        } catch (NoSuchFileException e) {
            return;
        }
        try (FileChannel dataChannel = FileChannel.open(dataPath, StandardOpenOption.READ);
             FileChannel indexChannel = FileChannel.open(indexPath, StandardOpenOption.READ)) {
            dataPages.add(dataChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataSize, arena));
            indexPages.add(indexChannel.map(FileChannel.MapMode.READ_ONLY, 0, indexSize, arena));
        }
    }

    public InMemoryDao(final Config config) throws IOException {
        this.basePath = config.basePath();
        arena = Arena.ofShared();

        final List<String> filenames = new ArrayList<>();
        try (final DirectoryStream<Path> paths =
                     Files.newDirectoryStream(basePath, "[0-9]+." + FN_DATA_SUFFIX)) {
            for (Path path : paths) {
                filenames.add(path.toString().substring(0, path.toString().length() - FN_DATA_SUFFIX.length() - 1));
            }
        }
        filenames.sort(Collections.reverseOrder());

        dataPages = new ArrayList<>();
        indexPages = new ArrayList<>();
        for (final String fn : filenames) {
            mapSSTable(fn);
        }
    }

    /**
     * Perform binary search in SSTable and return entry offset for last entry with key not greater than input {@code key}
     * @param key key to search for
     * @param sstableIndex SSTable index
     * @return offset in index page for last entry, which key less than or equal to {@code key}
     */
    private long sstableSearch(final MemorySegment key, final int sstableIndex) {
        final MemorySegment indexPage = indexPages.get(sstableIndex);
        final MemorySegment dataPage = dataPages.get(sstableIndex);
        long l = 0;
        long r = indexPage.byteSize() / (2 * Long.BYTES);
        long i = r / 2;
        while (i != l) {
            final int cmp = DaoUtils.compareMSandSSTKey(key, indexPage, 2 * Long.BYTES * i, dataPage);
            if (cmp < 0) {
                r = i;
            } else if (cmp > 0) {
                l = i;
            } else {
                return 2 * Long.BYTES * i;
            }
            i = (l + r) / 2;
        }
        return 2 * Long.BYTES * i;
    }

    /**
     * Performs binary search and checks whether given key actually presents in SSTable or marked as deleted
     *
     * @param key key to search for
     * @param iSST SSTable index
     * @return entry offset read from index page or {@code null} if key not found
     */
    private Long sstableGet(final MemorySegment key, final int iSST) {
        final long offset = sstableSearch(key, iSST);
        return DaoUtils.compareMSandSSTKey(dataPages.get(iSST), indexPages.get(iSST), offset, dataPages.get(iSST)) != 0 ?
                null : offset;
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(final MemorySegment from, final MemorySegment to) {
        final Iterator<Entry<MemorySegment>> memtableIterator;
        if (from == null && to == null) {
            memtableIterator = memtable.values().iterator();
        } else if (from == null) {
            memtableIterator = memtable.headMap(to).values().iterator();
        } else if (to == null) {
            memtableIterator = memtable.tailMap(from).values().iterator();
        } else {
            memtableIterator = memtable.subMap(from, to).values().iterator();
        }
        final List<Long> fromOffsets = new ArrayList<>();
        for (int i = 0; i < indexPages.size(); ++i) {
            long s = sstableSearch(from, i);
            if (DaoUtils.compareMSandSSTKey(from, indexPages.get(i), s, dataPages.get(i)) > 0) {
                s += 2 * Long.BYTES;
            }
            fromOffsets.add(s);
        }
        final List<Long> toOffsets = new ArrayList<>();
        for (int i = 0; i < indexPages.size(); ++i) {
            long s = sstableSearch(to, i);
            toOffsets.add(s);
        }
        return new InMemoryDaoIterator(memtableIterator, indexPages, dataPages, fromOffsets, toOffsets);
    }

    @Override
    public Entry<MemorySegment> get(final MemorySegment key) {
        final Entry<MemorySegment> found = memtable.get(key);
        if (found != null) {
            return found.value() == null ? null : found;
        }
        for (int i = 0; i < dataPages.size(); ++i) {
            final Long indexOffset = sstableGet(key, i);
            if (indexOffset != null) {
                if (indexOffset < 0) {
                    return null;
                }
                final long valueBeginOffset = indexPages.get(i).get(ValueLayout.JAVA_LONG_UNALIGNED,
                        indexOffset + Long.BYTES);
                final long valueEndOffset = DaoUtils.getEntryEndOffset(indexPages.get(i), indexOffset, dataPages.get(i));
                final MemorySegment value = dataPages.get(i).asSlice(valueBeginOffset,
                        valueEndOffset - valueBeginOffset);
                return new BaseEntry<>(key, value);
            }
        }
        return null;
    }

    @Override
    public void upsert(final Entry<MemorySegment> entry) {
        memtable.put(entry.key(), entry);
    }

    /**
     * Returns file name without extension for new SSTable in {@code basePath}.
     * For example, if {@code basePath} contains following files:
     * 0000001.data,
     * 0000001.idx,
     * 0000003.data,
     * 0000003.idx,
     * this function will return {@code "000004"}, i.e. max SSTable index + 1
     *
     * @return file name without extension for new SSTable
     * @throws IOException if an I/O error occurs
     */
    private String getNewSSTableName() throws IOException {
        String lastSSTable = "";
        try (final DirectoryStream<Path> paths =
                     Files.newDirectoryStream(basePath, "[0-9]+." + FN_DATA_SUFFIX)) {
            for (Path path : paths) {
                if (lastSSTable.compareTo(path.getFileName().toString()) < 0) {
                    lastSSTable = path.getFileName().toString();
                }
            }
        }
        final int newSSTableN = lastSSTable.isEmpty() ?
                1 : Integer.parseInt(lastSSTable.substring(0, lastSSTable.length() - FN_DATA_SUFFIX.length() - 1)) + 1;
        return String.valueOf(10_000_000 + newSSTableN).substring(1);
    }

    @Override
    public void close() throws IOException {
        if (!arena.scope().isAlive()) {
            return;
        }
        arena.close();

        final long indexBytes = 2L * memtable.size() * Long.BYTES;
        final long dataBytes = memtable.values().stream()
                .mapToLong(entry -> entry.key().byteSize() + (entry.value() == null ? 0 : entry.value().byteSize()))
                .sum();

        final String newSSTableName = getNewSSTableName();
        try (final Arena writeArena = Arena.ofConfined()) {
            MemorySegment indexPage;
            try (FileChannel fileChannel =
                         FileChannel.open(basePath.resolve(newSSTableName + "." + FN_INDEX_SUFFIX),
                                 StandardOpenOption.WRITE,
                                 StandardOpenOption.READ,
                                 StandardOpenOption.TRUNCATE_EXISTING,
                                 StandardOpenOption.CREATE)) {
                indexPage = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexBytes, writeArena);
            }
            MemorySegment dataPage;
            try (FileChannel fileChannel =
                         FileChannel.open(basePath.resolve(newSSTableName + "." + FN_DATA_SUFFIX),
                                 StandardOpenOption.WRITE,
                                 StandardOpenOption.READ,
                                 StandardOpenOption.TRUNCATE_EXISTING,
                                 StandardOpenOption.CREATE)) {
                dataPage = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataBytes, writeArena);
            }

            long indexOffset = 0;
            long dataOffset = 0;
            for (Entry<MemorySegment> entry : memtable.values()) {
                MemorySegment key = entry.key();
                MemorySegment value = entry.value();

                indexPage.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                indexOffset += Long.BYTES;
                MemorySegment.copy(key, 0, dataPage, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                indexPage.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, value == null ? -dataOffset : dataOffset);
                indexOffset += Long.BYTES;
                if (value != null) {
                    MemorySegment.copy(value, 0, dataPage, dataOffset, value.byteSize());
                    dataOffset += value.byteSize();
                }
            }
        }
    }
}
