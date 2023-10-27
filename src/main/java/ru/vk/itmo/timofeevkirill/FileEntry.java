package ru.vk.itmo.timofeevkirill;

import ru.vk.itmo.Entry;
import java.lang.foreign.MemorySegment;

record FileEntry(Entry<MemorySegment> entry, long number) {
}
