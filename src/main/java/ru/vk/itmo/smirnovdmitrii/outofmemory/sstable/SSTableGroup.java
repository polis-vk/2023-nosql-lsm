package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import java.util.HashSet;
import java.util.Set;

public class SSTableGroup {
    private final Set<SSTable> usedSSTables = new HashSet<>();

    public boolean register(final SSTable ssTable) {
        return usedSSTables.add(ssTable);
    }

    public void deregister(final SSTable ssTable) {
        usedSSTables.remove(ssTable);
    }
}
