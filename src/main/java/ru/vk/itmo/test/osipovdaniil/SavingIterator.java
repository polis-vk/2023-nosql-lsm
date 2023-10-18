package ru.vk.itmo.test.osipovdaniil;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Iterator;

public interface SavingIterator extends Iterator<Entry<MemorySegment>> {

    Entry<MemorySegment> getCurrEntry();
}
