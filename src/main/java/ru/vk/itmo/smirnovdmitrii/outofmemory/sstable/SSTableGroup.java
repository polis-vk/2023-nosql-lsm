package ru.vk.itmo.smirnovdmitrii.outofmemory.sstable;

import java.util.HashSet;

public class SSTableGroup {
    private final HashSet<SSTable> usedSSTables = new HashSet<>();

    public boolean register(final SSTable ssTable) {
        return usedSSTables.add(ssTable);
    }

    public void deregister(final SSTable ssTable) {
        usedSSTables.remove(ssTable);
    }
}
