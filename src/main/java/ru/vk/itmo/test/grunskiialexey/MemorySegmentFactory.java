package ru.vk.itmo.test.grunskiialexey;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;
import ru.vk.itmo.test.DaoFactory;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

@DaoFactory
public class MemorySegmentFactory implements DaoFactory.Factory<MemorySegment, Entry<MemorySegment>> {

    @Override
    public String toString(MemorySegment memorySegment) {
        // I use .asByteBuffer() because some properties like byte[] array will be linked and not copied
        // Also array() - just giving a link to the byte[] array inside ByteBuffer
        return new String(memorySegment.asByteBuffer().array(), StandardCharsets.UTF_8);
    }

    @Override
    public MemorySegment fromString(String data) {
        return MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return new BaseEntry<>(
                baseEntry.key(),
                baseEntry.value()
        );
    }

}
