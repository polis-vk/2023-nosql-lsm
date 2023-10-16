package ru.vk.itmo.proninvalentin;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;

// Хранит метаданные об Entry и Entry
public class EnrichedEntry {
    public final Metadata metadata;
    public final Entry<MemorySegment> entry;

    public EnrichedEntry(Metadata metadata, Entry<MemorySegment> entry) {
        this.metadata = metadata;
        this.entry = entry;
    }
}
