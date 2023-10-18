package ru.vk.itmo.test.kononovvladimir;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class FileIterator implements Iterator<Entry<MemorySegment>> {


    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Entry<MemorySegment> next() {
        return null;
    }
}
