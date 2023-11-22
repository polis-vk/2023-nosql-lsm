package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public class IterableDisk implements Iterable<Entry<MemorySegment>> {
    Environment environment;

    IterableDisk(Environment environment) {
        this.environment = environment;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return environment.range(null, null, Range.DISK);
    }
}