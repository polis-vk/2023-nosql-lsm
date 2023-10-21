package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SSTableWriter {

    protected static final StandardOpenOption[] options
            = {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

    private final Path filePath;
    private final Path indexFile;
    private final Path indexTemp;

    public SSTableWriter(Path path, Path indexFile, Path indexTemp) {
        filePath = path;

        this.indexFile = indexFile;
        this.indexTemp = indexTemp;
    }

    public void save(Collection<Entry<MemorySegment>> entries) throws IOException {
        if (entries.isEmpty()) {
            return;
        }

        if (!Files.exists(indexFile)) {
            Files.createFile(indexFile);
        }

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        String fileName = String.valueOf(existedFiles.size());

        long indexSize = 0L;
        long dataSize = 0L;

        for (Entry<MemorySegment> entry : entries) {
            MemorySegment value = entry.value();
            MemorySegment key = entry.key();

            dataSize += key.byteSize() + (value == null ? 0 : value.byteSize());
            indexSize += 2 * Long.BYTES;
        }

        try (Arena arenaWrite = Arena.ofConfined();
             FileChannel fileChannel = FileChannel.open(filePath.resolve(fileName), options)) {

            MemorySegment fileSegment = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, indexSize + dataSize, arenaWrite);

            long offset = 0L;
            long dataOffset = indexSize;

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment key = entry.key();
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, dataOffset);
                offset += Long.BYTES;
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();
                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value == null ? -1 : dataOffset);
                offset += Long.BYTES;
                dataOffset += value == null ? 0 : value.byteSize();
            }

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment key = entry.key();
                MemorySegment.copy(key, 0, fileSegment, offset, key.byteSize());
                offset += key.byteSize();

                MemorySegment value = entry.value();
                if (value != null) {
                    MemorySegment.copy(value, 0, fileSegment, offset, value.byteSize());
                    offset += value.byteSize();
                }
            }

        }

        Files.move(indexFile, indexTemp);

        List<String> info = new ArrayList<>(existedFiles.size() + 1);
        info.addAll(existedFiles);
        info.add(fileName);
        Files.write(indexFile, info, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        Files.delete(indexTemp);
    }
}