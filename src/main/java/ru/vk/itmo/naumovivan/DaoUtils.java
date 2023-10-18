package ru.vk.itmo.naumovivan;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class DaoUtils {
    static private int compareMemorySegmentsOffset(final MemorySegment ms1, final long fromOffset1, final long toOffset1,
                                                   final MemorySegment ms2, final long fromOffset2, final long toOffset2) {
        final long mismatch = MemorySegment.mismatch(ms1, fromOffset1, toOffset1, ms2, fromOffset2, toOffset2);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == toOffset1 - fromOffset1) {
            return -1;
        }
        if (mismatch == toOffset2 - fromOffset2) {
            return 1;
        }
        final byte b1 = ms1.get(ValueLayout.JAVA_BYTE, fromOffset1 + mismatch);
        final byte b2 = ms2.get(ValueLayout.JAVA_BYTE, fromOffset2 + mismatch);
        return Byte.compare(b1, b2);
    }

    static public int compareMemorySegments(final MemorySegment ms1, final MemorySegment ms2) {
        return compareMemorySegmentsOffset(ms1, 0, ms1.byteSize(), ms2, 0, ms2.byteSize());
    }

    static public int compareMSandSSTKey(final MemorySegment mtKey,
                                          final MemorySegment indexPage,
                                          final long offset,
                                          final MemorySegment dataPage) {
        final long fromOffset = indexPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
        final long toOffset = getEntryEndOffset(indexPage, offset, dataPage);
        return compareMemorySegmentsOffset(mtKey, 0, mtKey.byteSize(), dataPage, fromOffset, toOffset);
    }

    public static int compareSSTKeys(final MemorySegment indexPage1, final MemorySegment dataPage1, final long s1,
                                     final MemorySegment indexPage2, final MemorySegment dataPage2, final long s2) {
        final long fromOffset1 = indexPage1.get(ValueLayout.JAVA_LONG_UNALIGNED, s1);
        final long toOffset1 = getEntryEndOffset(indexPage1, s1, dataPage2);
        final long fromOffset2 = indexPage2.get(ValueLayout.JAVA_LONG_UNALIGNED, s2);
        final long toOffset2 = getEntryEndOffset(indexPage2, s2, dataPage2);
        return compareMemorySegmentsOffset(dataPage1, fromOffset1, toOffset1, dataPage2, fromOffset2, toOffset2);
    }

    static public long getEntryEndOffset(final MemorySegment indexPage, final long offset, final MemorySegment dataPage) {
        return offset + 2 * Long.BYTES == indexPage.byteSize() ?
                dataPage.byteSize() : indexPage.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 2 * Long.BYTES);
    }
}
