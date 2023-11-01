package ru.vk.itmo.reshetnikovaleksei;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.stream.Stream;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.iterators.MergeIterator;
import ru.vk.itmo.reshetnikovaleksei.iterators.PeekingIterator;

import static ru.vk.itmo.reshetnikovaleksei.SSTable.DATA_PREFIX;
import static ru.vk.itmo.reshetnikovaleksei.SSTable.INDEX_PREFIX;

public class SSTableManager implements AutoCloseable {

    private final Arena arena;
    private final Path basePath;
    private final List<SSTable> ssTables;

    private int lastIdx = 0;
    private boolean isClosed = false;

    public SSTableManager(Config config) throws IOException {
        this.arena = Arena.ofShared();
        this.basePath = config.basePath();
        this.ssTables = new ArrayList<>();

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
                lastIdx = i;
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

    public void save(Collection<Entry<MemorySegment>> entries) throws IOException {
        try (
                FileChannel dataChannel = FileChannel.open(
                        basePath.resolve(DATA_PREFIX + lastIdx),
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE
                );
                FileChannel indexChannel = FileChannel.open(
                        basePath.resolve(INDEX_PREFIX + lastIdx),
                        StandardOpenOption.READ,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE
                );
                Arena writeDataArena = Arena.ofConfined()
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
    }

    public void uploadDataFromFilesToMemory(ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memoryTable) {
        for (SSTable ssTable : ssTables) {
            Iterator<Entry<MemorySegment>> iterator = ssTable.iterator(null, null);
            while (iterator.hasNext()) {
                Entry<MemorySegment> entry = iterator.next();
                if (!memoryTable.containsKey(entry.key())) {
                    memoryTable.put(entry.key(), entry);
                }
            }
        }
    }

    public void deleteAllFiles() throws IOException {
        for (SSTable ssTable : ssTables) {
            ssTable.deleteFiles();
        }

        lastIdx = 0;
    }

    @Override
    public void close() {
        if (!isClosed) {
            arena.close();
            isClosed = true;
        }
    }
}
