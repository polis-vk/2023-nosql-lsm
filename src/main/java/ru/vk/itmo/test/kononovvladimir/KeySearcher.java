package ru.vk.itmo.test.kononovvladimir;

import java.lang.foreign.MemorySegment;

public class KeySearcher extends FileSearcher{
    public KeySearcher(MemorySegment memorySegment, long size) {
        super(memorySegment, size);
    }
}
