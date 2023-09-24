package ru.vk.itmo.test.svistukhinandrey;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Utils {
    public static String transform(MemorySegment memorySegment) {
        char[] chars = new char[(int) (memorySegment.byteSize() / 2)];

        for (int i = 0; i < memorySegment.byteSize() / 2; i++) {
            chars[i] = memorySegment.getAtIndex(ValueLayout.JAVA_CHAR, i);
        }
        return new String(chars);
    }


}
