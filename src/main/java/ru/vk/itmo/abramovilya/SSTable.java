package ru.vk.itmo.abramovilya;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

class SSTable implements Table {
    private final int number;
    private final MemorySegment mappedIndexFile;
    private final MemorySegment mappedStorageFile;
    private long offset;
    private MemorySegment currentKey;

    SSTable(int number, long offset, MemorySegment mappedStorageFile, MemorySegment mappedIndexFile) {
        this.number = number;
        this.offset = offset;
        this.mappedStorageFile = mappedStorageFile;
        this.mappedIndexFile = mappedIndexFile;

        long storageOffset = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        long msSize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, storageOffset);
        currentKey = mappedStorageFile.asSlice(storageOffset + Long.BYTES, msSize);
    }

    @Override
    public MemorySegment getKey() {
        return currentKey;
    }

    @Override
    public MemorySegment getValue() {
        long inStorageOffset = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        long keySize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, inStorageOffset);
        inStorageOffset += Long.BYTES;
        inStorageOffset += keySize;

        long valueSize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, inStorageOffset);
        if (valueSize == -1) {
            return null;
        }
        inStorageOffset += Long.BYTES;
        return mappedStorageFile.asSlice(inStorageOffset, valueSize);
    }


    @Override
    public MemorySegment nextKey() {
        offset += 2 * Long.BYTES;
        if (offset >= mappedIndexFile.byteSize()) {
            return null;
        }
        long storageOffset = mappedIndexFile.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);

        long msSize = mappedStorageFile.get(ValueLayout.JAVA_LONG_UNALIGNED, storageOffset);
        storageOffset += Long.BYTES;
        MemorySegment key = mappedStorageFile.asSlice(storageOffset, msSize);
        currentKey = key;
        return key;
    }

    @Override
    public int number() {
        return number;
    }
}
