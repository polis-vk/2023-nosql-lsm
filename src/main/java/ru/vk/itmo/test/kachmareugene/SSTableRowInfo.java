package ru.vk.itmo.test.kachmareugene;

public class SSTableRowInfo {
    long keyOffset;
    long valueOffset;
    long keySize;
    long valueSize;
    int SSTableInd;

    public SSTableRowInfo(long keyOffset, long valueOffset, long keySize, long valueSize, int SSTableInd) {
        this.keyOffset = keyOffset;
        this.valueOffset = valueOffset;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.SSTableInd = SSTableInd;
    }
}
