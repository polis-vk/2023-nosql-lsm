package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

public class MemorySegmentDao extends AbstractInMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final Serializer MEMORY_SEGMENT_SERIALIZER = new Serializer();

    public MemorySegmentDao(Config config) throws IOException {
        super(config, MemorySegmentComparator.INSTANCE, MEMORY_SEGMENT_SERIALIZER);
    }

    private static class Serializer implements MemorySegmentSerializer<MemorySegment, Entry<MemorySegment>> {
        @Override
        public MemorySegment toValue(MemorySegment input) {
            return input;
        }

        @Override
        public MemorySegment fromValue(MemorySegment value) {
            return value;
        }

        @Override
        public long size(MemorySegment value) {
            if (value == null) {
                return 0;
            }
            return value.byteSize();
        }

        @Override
        public Entry<MemorySegment> createEntry(MemorySegment key, MemorySegment value) {
            return new BaseEntry<>(key, value);
        }
    }
}

