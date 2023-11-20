package ru.vk.itmo.svistukhinandrey;

public class StorageState {
    private Memory activeSSTable;
    private Memory flushingSSTable;

    public StorageState(Memory activeSSTable, Memory flushingSSTable) {
        this.activeSSTable = activeSSTable;
        this.flushingSSTable = flushingSSTable;
    }

    public Memory getActiveSSTable() {
        return activeSSTable;
    }

    public void prepareStorageForFlush() {
        this.flushingSSTable = activeSSTable;
        this.activeSSTable = new Memory();
    }

    public boolean isReadyForFlush() {
        return flushingSSTable == null && !activeSSTable.getStorage().isEmpty();
    }

    public Memory getFlushingSSTable() {
        return flushingSSTable;
    }

    public static StorageState initStorageState() {
        return new StorageState(
                new Memory(),
                null
        );
    }
}
