package ru.vk.itmo.smirnovdmitrii.outofmemory;

import java.util.ArrayList;
import java.util.List;

/**
 * Representation of index file record/one line of index file.
 */
public class IndexFileRecord {
    private final boolean isSSTable;
    private final String name;
    private final long priority;
    private final List<Long> priorities;

    public IndexFileRecord(final String indexRecord) {
        @SuppressWarnings("StringSplitter")
        final String[] recordSplit = indexRecord.split(" ");
        this.isSSTable = recordSplit[0].equals("sstable");
        this.name = recordSplit[1];
        int compactedFirstIndex;
        if (recordSplit[1].equals("delete")) {
            this.priority = -1;
            compactedFirstIndex = 2;
        } else {
            this.priority = Long.parseLong(recordSplit[2]);
            compactedFirstIndex = 3;
        }
        if (isSSTable) {
            this.priorities = null;
            return;
        }
        this.priorities = new ArrayList<>();
        for (int i = compactedFirstIndex; i < recordSplit.length; i++) {
            this.priorities.add(Long.parseLong(recordSplit[i]));
        }
    }

    public boolean isCompaction() {
        return !isSSTable;
    }

    public boolean isSSTable() {
        return isSSTable;
    }

    public String getName() {
        return name;
    }

    public long getPriority() {
        return priority;
    }

    public List<Long> getCompactedPriorities() {
        return priorities;
    }
}
