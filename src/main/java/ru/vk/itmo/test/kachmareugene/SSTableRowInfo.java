package ru.vk.itmo.test.kachmareugene;

public class SSTableRowInfo {
    final long keyOffset;
    final long valueOffset;
    final long keySize;
    final long rowShift;
    private final long valueSize;
    final int ssTableInd;

    public SSTableRowInfo(long keyOffset, long keySize, long valueOffset,
                          long valueSize, int ssTableInd, long rowShift) {
        this.keyOffset = keyOffset;
        this.valueOffset = valueOffset;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.ssTableInd = ssTableInd;
        this.rowShift = rowShift;
    }

    public boolean isDeletedData() {
        return valueSize < 0;
    }

    public long getValueSize() {
        return valueSize;
    }
}
