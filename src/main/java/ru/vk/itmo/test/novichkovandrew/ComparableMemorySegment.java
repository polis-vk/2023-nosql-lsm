package ru.vk.itmo.test.novichkovandrew;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class ComparableMemorySegment implements Comparable<ComparableMemorySegment> {

    private final MemorySegment segment;

    public ComparableMemorySegment(MemorySegment segment) {
        this.segment = segment;
    }

    @Override
    public int compareTo(ComparableMemorySegment o) {
        if (this.segment == null || o == null) return -1;
        byte[] f = this.segment.toArray(ValueLayout.JAVA_BYTE);
        byte[] s = o.segment.toArray(ValueLayout.JAVA_BYTE);
        if (f.length != s.length) return Integer.compare(f.length, s.length);
        for (int i = 0; i < f.length; i++) {
            if (f[i] != s[i]) {
                return Integer.compare(f[i], s[i]);
            }
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ComparableMemorySegment that = (ComparableMemorySegment) o;
        return compareTo(that) == 0;
    }

    @Override
    public int hashCode() {
        return segment != null ? segment.hashCode() : 0;
    }
}
