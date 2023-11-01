package ru.vk.itmo.kovalevigor;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;

public class SStorageDumper extends Dumper {

    protected final IndexDumper indexDumper;
    private final MemorySegment keysSegment;
    private final MemorySegment valuesSegment;
    private long keysOffset = 0;
    private long valuesOffset = 0;
    private final Path tempKeysFile;
    private final Path tempValuesFile;

    protected SStorageDumper(
            final long entryCount,
            final long keysSize,
            final long valuesSize,
            final Path storagePath,
            final Path indexPath,
            final Arena arena
    ) throws IOException {
        super(storagePath, getSize(keysSize, valuesSize), arena);

        tempKeysFile = Files.createTempFile(null, null);
        tempValuesFile = Files.createTempFile(null, null);

        keysSegment = UtilsMemorySegment.mapWriteSegment(tempKeysFile, keysSize, arena);
        valuesSegment = UtilsMemorySegment.mapWriteSegment(tempValuesFile, valuesSize, arena);

        indexDumper = new IndexDumper(entryCount, indexPath, arena);
        indexDumper.setKeysSize(keysSize);
        indexDumper.setValuesSize(valuesSize);
    }

    public static long getSize(final long keysSize, final long valuesSize) {
        return keysSize + valuesSize;
    }

    public static long getIndexSize(final long entryCount) {
        return IndexDumper.getSize(entryCount);
    }

    public void setKeysSize(final long keysSize) {
        indexDumper.setKeysSize(keysSize);
    }

    public void setValuesSize(final long valuesSize) {
        indexDumper.setValuesSize(valuesSize);
    }

    @Override
    protected void writeHead() {
        indexDumper.writeHead();
    }

    private void writeKey(final MemorySegment segment) {
        final long size = segment.byteSize();
        MemorySegment.copy(
                segment,
                0,
                keysSegment,
                keysOffset,
                size
        );
        keysOffset += size;
    }

    private void writeValue(final MemorySegment segment) {
        final long size = segment.byteSize();
        MemorySegment.copy(
                segment,
                0,
                valuesSegment,
                valuesOffset,
                size
        );
        valuesOffset += size;
    }

    public void writeEntry(final Entry<MemorySegment> entry) {
        final long keyOffset = keysOffset;
        writeKey(entry.key());

        final long valueOffset;
        final MemorySegment valueSegment = entry.value();
        if (valueSegment == null) {
            valueOffset = -1;
        } else {
            valueOffset = valuesOffset;
            writeValue(valueSegment);
        }
        indexDumper.writeEntry(keyOffset, valueOffset);

    }

    @Override
    public void close() throws IOException {
        try {
            writeHead();
            offset = writeMemorySegment(keysSegment, offset, indexDumper.keysSize);
            offset = writeMemorySegment(valuesSegment, offset, indexDumper.valuesSize);
        } finally {
            try {
                indexDumper.close();
            } finally {
                try {
                    Files.deleteIfExists(tempKeysFile);
                } finally {
                    Files.deleteIfExists(tempValuesFile);
                }
            }
        }
    }
}
