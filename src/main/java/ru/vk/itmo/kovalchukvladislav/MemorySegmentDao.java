package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Comparator;

public class MemorySegmentDao extends AbstractInMemoryDao<MemorySegment, Entry<MemorySegment>> {
    private static final ValueLayout.OfByte VALUE_LAYOUT = ValueLayout.JAVA_BYTE;
    private static final Serializer MEMORY_SEGMENT_SERIALIZER = new Serializer();
    private static final Comparator<? super MemorySegment> COMPARATOR = getComparator();

    public MemorySegmentDao(Config config) {
        super(config, COMPARATOR, MEMORY_SEGMENT_SERIALIZER);
    }

    private static Comparator<? super MemorySegment> getComparator() {
        return (Comparator<MemorySegment>) (a, b) -> {
            long diffIndex = a.mismatch(b);
            if (diffIndex == -1) {
                return 0;
            } else if (diffIndex == a.byteSize()) {
                return -1;
            } else if (diffIndex == b.byteSize()) {
                return 1;
            }

            byte byteA = a.getAtIndex(VALUE_LAYOUT, diffIndex);
            byte byteB = b.getAtIndex(VALUE_LAYOUT, diffIndex);
            return Byte.compare(byteA, byteB);
        };
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
            return value.byteSize();
        }

        @Override
        public Entry<MemorySegment> createEntry(MemorySegment key, MemorySegment value) {
            return new BaseEntry<>(key, value);
        }
    }
}

