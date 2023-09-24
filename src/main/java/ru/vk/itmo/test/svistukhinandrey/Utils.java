package ru.vk.itmo.test.svistukhinandrey;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

public class Utils {
    public static String transform(MemorySegment memorySegment) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < memorySegment.byteSize(); i++) {
            char a = (char) memorySegment.get(ValueLayout.JAVA_BYTE, i);
            if (a != 0) {
                stringBuilder.append((char) memorySegment.get(ValueLayout.JAVA_BYTE, i));
            }
        }
        return stringBuilder.toString();
    }


}
