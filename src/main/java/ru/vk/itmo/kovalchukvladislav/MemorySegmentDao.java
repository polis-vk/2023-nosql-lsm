package ru.vk.itmo.kovalchukvladislav;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

public class MemorySegmentDao extends AbstractBasedOnSSTableDao<MemorySegment, Entry<MemorySegment>> {
    public MemorySegmentDao(Config config) throws IOException {
        super(config, MemorySegmentEntryExtractor.INSTANCE);
    }
}
