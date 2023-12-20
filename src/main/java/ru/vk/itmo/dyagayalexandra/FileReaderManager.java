package ru.vk.itmo.dyagayalexandra;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class FileReaderManager {

    private final MemorySegmentComparator memorySegmentComparator;

    public FileReaderManager(MemorySegmentComparator memorySegmentComparator) {
        this.memorySegmentComparator = memorySegmentComparator;
    }

    long getIndexSize(MemorySegment indexMemorySegment) {
        return indexMemorySegment.get(ValueLayout.JAVA_LONG_UNALIGNED, 0);
    }

    long getEntryIndex(MemorySegment ssTable, MemorySegment ssIndex,
                       MemorySegment key, long indexSize) throws IOException {
        long low = 0;
        long high = indexSize - 1;
        long mid = (low + high) / 2;
        while (low <= high) {
            Entry<MemorySegment> current = getCurrentEntry(mid, ssTable, ssIndex);
            int compare = memorySegmentComparator.compare(key, current.key());
            if (compare > 0) {
                low = mid + 1;
            } else if (compare < 0) {
                high = mid - 1;
            } else {
                return mid;
            }
            mid = (low + high) / 2;
        }

        return low;
    }

    Entry<MemorySegment> getCurrentEntry(long position, MemorySegment ssTable,
                                         MemorySegment ssIndex) throws IOException {
        long offset = ssIndex.get(ValueLayout.JAVA_LONG_UNALIGNED, (position + 1) * Long.BYTES);

        int keyLength = ssTable.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += Integer.BYTES;

        MemorySegment keyMemorySegment = ssTable.asSlice(offset, keyLength);
        offset += keyLength;

        int valueLength = ssTable.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
        offset += Integer.BYTES;

        if (valueLength == -1) {
            return new BaseEntry<>(keyMemorySegment, null);
        }

        MemorySegment valueMemorySegment = ssTable.asSlice(offset, valueLength);

        return new BaseEntry<>(keyMemorySegment, valueMemorySegment);
    }
}
