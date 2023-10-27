package ru.vk.itmo.timofeevkirill;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;

public class MSNumberComparator implements Comparator<FileEntry> {
    Comparator<MemorySegment> comparator;

    public MSNumberComparator(Comparator<MemorySegment> comparator) {
        this.comparator = comparator;
    }

    @Override
    public int compare(FileEntry e1, FileEntry e2) {
        int compareResult = comparator.compare(e1.entry().key(), e2.entry().key());
        if (compareResult == 0) {
            return Long.compare(e2.number(), e1.number());
        }
        return compareResult;
    }
}
