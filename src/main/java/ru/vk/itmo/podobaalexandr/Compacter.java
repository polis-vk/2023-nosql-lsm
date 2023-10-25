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
import java.util.ArrayList;
import java.util.List;

public class Compacter {

    private final Path path;
    private final Path indexFile;
    private final Path indexTemp;

    private final SSTableReader ssTableReader;

    public Compacter(Path path, Path indexFile, Path indexTemp, SSTableReader ssTableReader) {
        this.path = path;
        this.indexFile = indexFile;
        this.indexTemp = indexTemp;
        this.ssTableReader = ssTableReader;
    }

    /** Read count of entries, create new file and fill data from whole mapped data.
     * Writing is made as in SSTableWriter.save(), but if first file of index equals a new one than to a new add 'c'.
     * @param priorityIterator - iterator to read entries
     * @param entries - iterator to find number of entries
     */
    public void compact(PriorityIterator priorityIterator, PriorityIterator entries) throws IOException {

        List<String> existedFiles = Files.readAllLines(indexFile, StandardCharsets.UTF_8);
        String fileName = String.valueOf(existedFiles.size());

        if (fileName.equals(existedFiles.get(0))) {
            fileName = "compacted";
        }

        long entriesCount = 0;
        long dataSize = 0;

        //find number of entries
        while (entries.hasNext()) {
            Entry<MemorySegment> entry = entries.next();
            dataSize += entry.key().byteSize() + entry.value().byteSize();
            entriesCount++;
        }

        long indexSize = entriesCount * 2 * Long.BYTES;

        //write data
        try (
                Arena arenaCompact = Arena.ofConfined();
                FileChannel fileChannel = FileChannel.open(path.resolve(fileName), SSTableWriter.options)
        ) {
            MemorySegment fileSegment =
                    fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, indexSize + dataSize, arenaCompact);

            long indexOffset = 0L;
            long dataOffset = indexSize;

            while (priorityIterator.hasNext()) {
                Entry<MemorySegment> entry = priorityIterator.next();

                MemorySegment key = entry.key();

                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                indexOffset += Long.BYTES;
                MemorySegment.copy(key, 0, fileSegment, dataOffset, key.byteSize());
                dataOffset += key.byteSize();

                MemorySegment value = entry.value();

                fileSegment.set(ValueLayout.JAVA_LONG_UNALIGNED, indexOffset, dataOffset);
                indexOffset += Long.BYTES;
                MemorySegment.copy(value, 0, fileSegment, dataOffset, value.byteSize());
                dataOffset += value.byteSize();
            }
        }

        Files.move(indexFile, indexTemp);
        List<String> info = new ArrayList<>(1);
        info.add(fileName);
        Files.write(indexFile, info, SSTableWriter.optionsWriteIndex);
        Files.delete(indexTemp);

        ssTableReader.close();
        for (String existedFile: existedFiles) {
            Files.delete(path.resolve(existedFile));
        }
    }
}
