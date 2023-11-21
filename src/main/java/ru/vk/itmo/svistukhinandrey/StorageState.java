package ru.vk.itmo.svistukhinandrey;

public class StorageState {
    private SSTable activeSSTable;
    private SSTable flushingSSTable;

    public StorageState(SSTable activeSSTable, SSTable flushingSSTable) {
        this.activeSSTable = activeSSTable;
        this.flushingSSTable = flushingSSTable;
    }

    public SSTable getActiveSSTable() {
        return activeSSTable;
    }

    public SSTable getFlushingSSTable() {
        return flushingSSTable;
    }

    public boolean isReadyForFlush() {
        return flushingSSTable == null && !activeSSTable.getStorage().isEmpty();
    }

    public synchronized void prepareStorageForFlush() {
        this.flushingSSTable = activeSSTable;
        this.activeSSTable = new SSTable();
    }

    public synchronized void removeFlushingSSTable() {
        this.flushingSSTable = null;
    }

    public static StorageState initStorageState() {
        return new StorageState(
                new SSTable(),
                null
        );
    }
}
