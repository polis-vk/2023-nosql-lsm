package ru.vk.itmo.podobaalexandr;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Comparator;

public class SSTable {

    private static final String FILE_NAME = "text.txt";

    private final Path filePath;

    private final Comparator<MemorySegment> comparatorMem;

    public SSTable(Path path, Comparator<MemorySegment> comparator) {
        filePath = path;
        comparatorMem = comparator;
    }

    public Entry<MemorySegment> get(MemorySegment keySearch) {
        MemorySegment fileData;
        if (!filePath.resolve(FILE_NAME).toFile().exists()) {
            return null;
        }

        OpenOption[] options = {StandardOpenOption.READ, StandardOpenOption.WRITE};

        try(FileChannel fileChannel = FileChannel.open(filePath.resolve(FILE_NAME), options)) {
            fileData = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, fileChannel.size(), Arena.ofAuto());

            long offset = 0L;
            while (offset < fileChannel.size()) {
                long keySize = fileData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;
                MemorySegment key = fileData.asSlice(offset, keySize);
                offset += keySize;

                long valueSize = fileData.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += Long.BYTES;

                if (comparatorMem.compare(keySearch, key) == 0) {
                    MemorySegment value = fileData.asSlice(offset, valueSize);
                    return new BaseEntry<>(key, value);
                }

                offset += valueSize;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    
    public void save(Collection<Entry<MemorySegment>> entries) {

        long size = 0;

        for (Entry<MemorySegment> entry : entries) {
            size += 2 * Long.BYTES + entry.key().byteSize() + entry.value().byteSize();
        }

        OpenOption[] options = {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

        try(FileChannel fileChannel = FileChannel.open(filePath.resolve(FILE_NAME), options)) {
            MemorySegment fileSegment = fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, size, Arena.ofAuto());

            long offset = 0L;

            for (Entry<MemorySegment> entry : entries) {
                MemorySegment slice = fileSegment.asSlice(offset, Long.BYTES);
                slice.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entry.key().byteSize());

                offset += Long.BYTES;

                slice = fileSegment.asSlice(offset, entry.key().byteSize());
                MemorySegment.copy(entry.key(), 0, slice, 0, entry.key().byteSize());

                offset += entry.key().byteSize();

                slice = fileSegment.asSlice(offset, Long.BYTES);
                slice.set(ValueLayout.JAVA_LONG_UNALIGNED, 0, entry.value().byteSize());

                offset += Long.BYTES;

                slice = fileSegment.asSlice(offset, entry.value().byteSize());
                MemorySegment.copy(entry.value(), 0, slice, 0, entry.value().byteSize());

                offset += entry.value().byteSize();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
