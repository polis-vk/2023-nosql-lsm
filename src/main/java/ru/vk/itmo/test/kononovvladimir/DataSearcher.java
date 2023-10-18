package ru.vk.itmo.test.kononovvladimir;

import java.lang.foreign.MemorySegment;

public class DataSearcher extends FileSearcher{
    public DataSearcher(MemorySegment memorySegment, long size) {
        super(memorySegment, size);
    }

}
