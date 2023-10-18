package ru.vk.itmo.reshetnikovaleksei;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;
import ru.vk.itmo.reshetnikovaleksei.iterator.MergeIterator;
import ru.vk.itmo.reshetnikovaleksei.iterator.PeekingIterator;
import ru.vk.itmo.reshetnikovaleksei.iterator.SSTableIterator;
import ru.vk.itmo.reshetnikovaleksei.utils.SSTableUtils;

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
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class SSTable {
    private static final String SS_TABLE_NAME = "ss_table_";

    private final Path basePath;
    private final Arena readDataArena;
    private final List<MemorySegment> readDataSegments;
    private final Comparator<MemorySegment> comparator;

    public SSTable(Config config) throws IOException {
        this.basePath = config.basePath();
        this.readDataArena = Arena.ofConfined();
        this.readDataSegments = new ArrayList<>();
        this.comparator = new MemorySegmentComparator();

        Pattern filePriorityPattern = Pattern.compile(SS_TABLE_NAME + "(\\d+)");
        try (Stream<Path> fileStream = Files.list(basePath)) {
            fileStream
                    .filter(ssTablePath -> filePriorityPattern.matcher(ssTablePath.getFileName().toString()).matches())
                    .sorted(
                            Comparator.comparingInt(path -> {
                                Matcher matcher = filePriorityPattern.matcher(path.getFileName().toString());
                                if (matcher.find()) {
                                    return Integer.parseInt(matcher.group(1));
                                }

                                throw new RuntimeException("Something wrong with file %s"
                                        .formatted(path.getFileName().toString()));
                            })
                    )
                    .forEach(
                            ssTablePath -> {
                                try (FileChannel ssTableChannel = FileChannel
                                        .open(ssTablePath, StandardOpenOption.READ)) {
                                    readDataSegments.add(
                                            ssTableChannel.map(
                                                    FileChannel.MapMode.READ_ONLY, 0,
                                                    ssTableChannel.size(), readDataArena
                                            )
                                    );
                                } catch (IOException e) {
                                    readDataArena.close();
                                    throw new RuntimeException(e);
                                }
                            }
                    );
        }
    }

    public Iterator<Entry<MemorySegment>> allFilesIterator(MemorySegment from, MemorySegment to) {
        List<PeekingIterator> iterators = new ArrayList<>();

        int priority = 1;
        for (MemorySegment readDataSegment : readDataSegments) {
            iterators.add(new PeekingIterator(oneFileIterator(readDataSegment, from, to), priority));
            priority++;
        }

        return MergeIterator.merge(iterators, comparator);
    }

    private Iterator<Entry<MemorySegment>> oneFileIterator(
            MemorySegment ssTable, MemorySegment from, MemorySegment to) {
        long indexFrom;
        long indexTo;

        if (from == null && to == null) {
            indexFrom = 0;
            indexTo = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        } else if (from == null) {
            indexFrom = 0;
            indexTo = SSTableUtils.binarySearch(ssTable, to);
        } else if (to == null) {
            indexFrom = SSTableUtils.binarySearch(ssTable, from);
            indexTo = ssTable.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
        } else {
            indexFrom = SSTableUtils.binarySearch(ssTable, from);
            indexTo = SSTableUtils.binarySearch(ssTable, to);
        }

        return new SSTableIterator(indexTo, ssTable, indexFrom);
    }

    public void save(Collection<Entry<MemorySegment>> entries) throws IOException {
        try (FileChannel ssTableChannel = FileChannel.open(
                basePath.resolve(SS_TABLE_NAME + readDataSegments.size() + 1),
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
             Arena writeDataArena = Arena.ofConfined()
        ) {
            long size = 0;
            for (Entry<MemorySegment> entry : entries) {
                size += entry.key().byteSize() + entry.value().byteSize() + 2 * Long.BYTES;
            }
            size += Long.BYTES + Long.BYTES * (long) entries.size();

            long offset = 0;
            MemorySegment ssTableMemorySegment = ssTableChannel.map(
                    FileChannel.MapMode.READ_WRITE, offset, size, writeDataArena);
            ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entries.size());
            offset += Long.BYTES + Long.BYTES * (long) entries.size();
            long i = 0;
            for (Entry<MemorySegment> entry : entries) {
                ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES + i * Byte.SIZE, offset);
                offset += setMemorySegmentSizeAndGetOffset(offset, entry.key(), ssTableMemorySegment);
                offset += copyToMemorySegmentAndGetOffset(offset, entry.key(), ssTableMemorySegment);
                
                offset += setMemorySegmentSizeAndGetOffset(offset, entry.value(), ssTableMemorySegment);
                offset += copyToMemorySegmentAndGetOffset(offset, entry.value(), ssTableMemorySegment);

                i++;
            }
        }
    }

    public void close() {
        if (readDataArena != null) {
            readDataArena.close();
        }
    }

    private long setMemorySegmentSizeAndGetOffset(long offset,
                                                  MemorySegment entryMemorySegment,
                                                  MemorySegment ssTableMemorySegment) {
        ssTableMemorySegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, entryMemorySegment.byteSize());
        return Long.BYTES;
    }

    private long copyToMemorySegmentAndGetOffset(long offset,
                                                 MemorySegment entryMemorySegment,
                                                 MemorySegment ssTableMemorySegment) {
        MemorySegment.copy(entryMemorySegment, 0, ssTableMemorySegment, offset, entryMemorySegment.byteSize());
        return entryMemorySegment.byteSize();
    }
}
