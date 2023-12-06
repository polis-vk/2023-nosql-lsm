package ru.vk.itmo.bandurinvladislav;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
  My simplified implementation of BitSet for getting long[] array without copying data
  (Default java implementation doesn't have this option)
*/
public class BitSet {

    private final List<Long> bitset;

    public BitSet(int capacity) {
        this.bitset = new ArrayList<>(Collections.nCopies(capacity, 0L));
    }

    public void set(int i) {
        int arrayOffset = i >> 6;
        int longOffset = i - (arrayOffset << 6);

        bitset.set(arrayOffset, bitset.get(arrayOffset) | (1L << (63 - longOffset)));
    }

    public boolean get(int i) {
        int arrayOffset = i >> 6;
        int longOffset = i - (arrayOffset << 6);

        return ((1L << (63 - longOffset)) & bitset.get(arrayOffset)) != 0;
    }

    public List<Long> getBitset() {
        return Collections.unmodifiableList(bitset);
    }

    public int size() {
        return bitset.size();
    }
}
