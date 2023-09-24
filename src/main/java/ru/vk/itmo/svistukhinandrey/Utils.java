package ru.vk.itmo.svistukhinandrey;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class Utils {

    private Utils() {}

    private static final Iterator<Entry<MemorySegment>> emptyIterator = new Iterator<>() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Entry<MemorySegment> next() {
            throw new NoSuchElementException();
        }
    };

    public static Iterator<Entry<MemorySegment>> getEmptyIterator() {
        return emptyIterator;
    }

    public static String memorySegmentToString(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }

        return new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }
}
