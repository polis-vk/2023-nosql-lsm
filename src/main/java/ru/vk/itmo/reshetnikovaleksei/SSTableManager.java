package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.iterators.MergeIterator;
import ru.vk.itmo.reshetnikovaleksei.iterators.PeekingIterator;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static ru.vk.itmo.reshetnikovaleksei.SSTable.COMPACTED_DATA;
import static ru.vk.itmo.reshetnikovaleksei.SSTable.COMPACTED_INDEX;
import static ru.vk.itmo.reshetnikovaleksei.SSTable.COMPACTED_PREFIX;
import static ru.vk.itmo.reshetnikovaleksei.SSTable.DATA_PREFIX;
import static ru.vk.itmo.reshetnikovaleksei.SSTable.DATA_TMP;
import static ru.vk.itmo.reshetnikovaleksei.SSTable.INDEX_PREFIX;
import static ru.vk.itmo.reshetnikovaleksei.SSTable.INDEX_TMP;


public class SSTableManager implements Iterable<Entry<MemorySegment>> {
    private static final Pattern COMPACTED_PATTERN = Pattern.compile(COMPACTED_PREFIX + "\\d*");

    private final Arena arena;
    private final Path basePath;
    private final List<SSTable> ssTables;
    private final AtomicLong lastIdx;
    private final AtomicBoolean isClosed;

    public SSTableManager(Config config) throws IOException {
        this.arena = Arena.ofShared();
        this.basePath = config.basePath();
        this.ssTables = new CopyOnWriteArrayList<>();
        this.lastIdx = new AtomicLong(0);
        this.isClosed = new AtomicBoolean(false);

        if (!Files.exists(basePath)) {
            return;
        }

        long filesCount;
        try (Stream<Path> filesStream = Files.list(basePath)) {
            filesCount = filesStream.count();
        }

        for (int i = 0; i < filesCount; i++) {
            try {
                ssTables.add(new SSTable(basePath, arena, i));
            } catch (IOException e) {
                lastIdx.set(i);
            }
        }
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment key) {
        return get(key, null);
    }

    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        List<PeekingIterator> iterators = new ArrayList<>();

        int priority = 1;
        for (SSTable ssTable : ssTables) {
            iterators.add(new PeekingIterator(ssTable.iterator(from, to), priority));
            priority++;
        }

        return MergeIterator.merge(iterators, MemorySegmentComparator.getInstance());
    }

    private void save(Iterable<Entry<MemorySegment>> entries, boolean isCompacting) throws IOException {
        Path tmpDataPath = basePath.resolve(DATA_TMP);
        Path tmpIndexPath = basePath.resolve(INDEX_TMP);

        Path dataPath;
        Path indexPath;
        if (isCompacting) {
            dataPath = basePath.resolve(COMPACTED_DATA);
            indexPath = basePath.resolve(COMPACTED_INDEX);
        } else {
            dataPath = basePath.resolve(DATA_PREFIX + lastIdx);
            indexPath = basePath.resolve(INDEX_PREFIX + lastIdx);
        }

        try (
                FileChannel dataChannel = FileChannel.open(
                        tmpDataPath,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE
                );
                FileChannel indexChannel = FileChannel.open(
                        tmpIndexPath,
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE
                );
                Arena writeDataArena = Arena.ofShared()
        ) {
            long dataSize = Long.BYTES;
            long indexSize = 0;
            for (Entry<MemorySegment> entry : entries) {
                dataSize += entry.key().byteSize()
                        + (entry.value() == null ? 0 : entry.value().byteSize())
                        + 2 * Long.BYTES;
                indexSize += Long.BYTES;
            }

            long dataOffset = 0;
            long indexOffset = 0;

            MemorySegment dataSegment = dataChannel.map(
                    FileChannel.MapMode.READ_WRITE, dataOffset, dataSize, writeDataArena);
            dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, dataSize - Long.BYTES);
            dataOffset += Long.BYTES;

            MemorySegment indexSegment = indexChannel.map(
                    FileChannel.MapMode.READ_WRITE, indexOffset, indexSize, writeDataArena);

            for (Entry<MemorySegment> entry : entries) {
                indexSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                indexOffset += Long.BYTES;

                dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, entry.key().byteSize());
                dataOffset += Long.BYTES;
                MemorySegment.copy(entry.key(), 0, dataSegment, dataOffset, entry.key().byteSize());
                dataOffset += entry.key().byteSize();

                if (entry.value() == null) {
                    dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, -1);
                    dataOffset += Long.BYTES;
                } else {
                    dataSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, dataOffset, entry.value().byteSize());
                    dataOffset += Long.BYTES;
                    MemorySegment.copy(entry.value(), 0, dataSegment, dataOffset, entry.value().byteSize());
                    dataOffset += entry.value().byteSize();
                }
            }
        }

        moveDataFromTmpToReal(tmpDataPath, dataPath);
        moveDataFromTmpToReal(tmpIndexPath, indexPath);
    }

    public void flush(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        save(entries, false);
        ssTables.addFirst(new SSTable(basePath, arena, lastIdx.get()));
        lastIdx.getAndAdd(1);
    }

    public void compact() throws IOException {
        if (!iterator().hasNext()) {
            return;
        }

        save(this, true);

        try (Stream<Path> files = Files.list(basePath)) {
            files
                    .filter(path -> !COMPACTED_PATTERN.matcher(path.toString()).find())
                    .forEach(tablePath -> {
                        try {
                            Files.delete(tablePath);
                        } catch (IOException e) {
                            throw new IllegalArgumentException("Can't delete file", e);
                        }
                    });
        } catch (IOException e) {
            throw new IllegalArgumentException("Can't access the directory " + basePath, e);
        }
        lastIdx.set(0);

        moveDataFromTmpToReal(basePath.resolve(COMPACTED_DATA), basePath.resolve(DATA_PREFIX + lastIdx));
        moveDataFromTmpToReal(basePath.resolve(COMPACTED_INDEX), basePath.resolve(INDEX_PREFIX + lastIdx));

        ssTables.clear();
        ssTables.add(new SSTable(basePath, arena, lastIdx.get()));
    }

    public void close() {
        if (!isClosed.get()) {
            arena.close();
            isClosed.set(true);
        }
    }

    private void moveDataFromTmpToReal(Path tmpFilePath, Path realFilePath) throws IOException {
        try {
            Files.createFile(realFilePath);
        } catch (FileAlreadyExistsException ignored) {
            // do nothing
        }

        Files.move(tmpFilePath, realFilePath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return get(null, null);
    }
}
