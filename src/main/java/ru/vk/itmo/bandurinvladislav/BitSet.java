package ru.vk.itmo.bandurinvladislav;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/*
  My simplified implementation of BitSet for getting long[] array without copying data
  (Default java implementation doesn't have this option)
*/
public class BitSet {

    private final List<Long> longs;

    public BitSet(int capacity) {
        this.longs = new ArrayList<>(Collections.nCopies(capacity, 0L));
    }

    public void set(int i) {
        int arrayOffset = i >> 6;
        int longOffset = i - (arrayOffset << 6);

        longs.set(arrayOffset, longs.get(arrayOffset) | (1L << (63 - longOffset)));
    }

    public boolean get(int i) {
        int arrayOffset = i >> 6;
        int longOffset = i - (arrayOffset << 6);

        return ((1L << (63 - longOffset)) & longs.get(arrayOffset)) != 0;
    }

    public List<Long> getLongs() {
        return Collections.unmodifiableList(longs);
    }

    public int size() {
        return longs.size();
    }
}
