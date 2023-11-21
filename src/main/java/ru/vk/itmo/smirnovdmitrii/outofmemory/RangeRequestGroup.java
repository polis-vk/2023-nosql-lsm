package ru.vk.itmo.smirnovdmitrii.outofmemory;

import ru.vk.itmo.smirnovdmitrii.outofmemory.sstable.SSTable;

import java.util.HashSet;
import java.util.Set;

/**
 * Group of SSTable which participant in range request.
 */
public class RangeRequestGroup {
    private final Set<SSTable> participants = new HashSet<>();

    /**
     * Register SSTable to RangeRequestGroup.
     * @param ssTable SSTable to register.
     * @return false if SSTable has already been registered, true otherwise.
     */
    public boolean register(final SSTable ssTable) {
        return participants.add(ssTable);
    }

    /**
     * Deregister SSTable from group.
     * @param ssTable SSTable to deregister.
     */
    public void deregister(final SSTable ssTable) {
        participants.remove(ssTable);
    }

}
