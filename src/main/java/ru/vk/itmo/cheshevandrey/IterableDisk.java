package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.Iterator;

public class IterableDisk implements Iterable<Entry<MemorySegment>> {
    Environment environment;
    Range range;

    IterableDisk(Environment environment, Range range) {
        this.environment = environment;
        this.range = range;
    }

    @Override
    public Iterator<Entry<MemorySegment>> iterator() {
        return environment.range(
                Collections.emptyIterator(),
                Collections.emptyIterator(),
                null,
                null,
                range
        );
    }
}