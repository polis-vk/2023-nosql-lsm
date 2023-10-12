package ru.vk.itmo.proninvalentin;

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
import java.util.Iterator;
import java.util.List;

public class FileIterator {
    // public static List<Iterator<MemorySegment>> createMany(Path directoryPath, String filePrefix) {
    //
    //          * Нахожу все файлы с указанным префиксом
    //          * Для каждого файла
    //          *
    //         List<File> filesWithPrefix = FileUtils.getAllFilesWithPrefix(directoryPath, filePrefix);
    //         if (filesWithPrefix.isEmpty()) {
    //             return new ArrayList<>();
    //         }
    //
    //         var iterators = new ArrayList<Iterator<MemorySegment>>(filesWithPrefix.size());
    //
    //         iterators.add();
    //
    //         return iterators;
    //     }

    public static Iterator<Entry<MemorySegment>> createIteratorForFile(Path valuesFilePath,
                                                                      Path offsetsFilePath,
                                                                      Arena readArena,
                                                                      long offset,
                                                                      long iterStep) throws IOException {
        MemorySegment readValuesMS;
        MemorySegment readOffsetsMS;
        try (FileChannel valuesChannel = FileChannel.open(valuesFilePath, StandardOpenOption.READ);
             FileChannel offsetsChannel = FileChannel.open(offsetsFilePath, StandardOpenOption.READ)) {
            readValuesMS = valuesChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    valuesChannel.size(), readArena);
            readOffsetsMS = offsetsChannel.map(FileChannel.MapMode.READ_ONLY, 0,
                    offsetsChannel.size(), readArena);
        }

        return new Iterator<>() {
            private long curOffset = offset;
            private final long fileSize = Files.size(offsetsFilePath);

            @Override
            public boolean hasNext() {
                return curOffset + iterStep < fileSize;
            }

            @Override
            public Entry<MemorySegment> next() {
                curOffset += iterStep;

                if (readValuesMS == null || readOffsetsMS == null) {
                    return null;
                }

                long keySizeOffset = readOffsetsMS.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                MemorySegment key = Utils.getBySizeOffset(readValuesMS, keySizeOffset);
                long valueSizeOffset = keySizeOffset + Long.BYTES + key.byteSize();
                MemorySegment value = Utils.getBySizeOffset(readValuesMS, valueSizeOffset);

                return new BaseEntry<>(
                        MemorySegment.ofArray(key.toArray(ValueLayout.JAVA_BYTE)),
                        MemorySegment.ofArray(value.toArray(ValueLayout.JAVA_BYTE)));
            }
        };
    }
}
