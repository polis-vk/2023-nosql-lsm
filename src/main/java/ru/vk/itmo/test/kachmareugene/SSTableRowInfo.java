package ru.vk.itmo.test.kachmareugene;

public class SSTableRowInfo {
    long keyOffset;
    long valueOffset;
    long keySize;
    long rowShift;
    private long valueSize;
    int SSTableInd;

    public SSTableRowInfo(long keyOffset, long keySize, long valueOffset, long valueSize, int SSTableInd, long rowShift) {
        this.keyOffset = keyOffset;
        this.valueOffset = valueOffset;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.SSTableInd = SSTableInd;
        this.rowShift = rowShift;
    }

    public boolean isDeletedData() {
        return valueSize < 0;
    }

    public long getValueSize() {
        return valueSize;
    }

    public long totalShift() {
        return keyOffset + keySize + valueSize;
    }
}
