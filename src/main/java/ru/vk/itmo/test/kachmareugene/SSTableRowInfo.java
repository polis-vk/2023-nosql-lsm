package ru.vk.itmo.test.kachmareugene;

public class SSTableRowInfo {
    long keyOffset;
    long valueOffset;
    long keySize;
    long rowShift;
    private final long valueSize;
    int ssTableInd;
    boolean isReversedToIter;

    public SSTableRowInfo(long keyOffset, long keySize, long valueOffset,
                          long valueSize, int ssTableInd, long rowShift) {
        this.keyOffset = keyOffset;
        this.valueOffset = valueOffset;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.ssTableInd = ssTableInd;
        this.rowShift = rowShift;
    }

    public SSTableRowInfo(long keyOffset, long keySize, long valueOffset,
                          long valueSize, int ssTableInd, long rowShift, boolean isReversedToIter) {
        this.keyOffset = keyOffset;
        this.valueOffset = valueOffset;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.ssTableInd = ssTableInd;
        this.rowShift = rowShift;
        this.isReversedToIter = isReversedToIter;
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
