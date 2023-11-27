package ru.vk.itmo.kovalchukvladislav.model;

public class TableInfo {
    private final long recordsCount;
    private final long recordsSize;

    public TableInfo(long recordsCount, long recordsSize) {
        this.recordsCount = recordsCount;
        this.recordsSize = recordsSize;
    }

    public long getRecordsCount() {
        return recordsCount;
    }

    public long getRecordsSize() {
        return recordsSize;
    }

    @Override
    public String toString() {
        return "TableInfo{"
                + "recordsCount=" + recordsCount
                + ", recordsSize=" + recordsSize
                + '}';
    }
}
