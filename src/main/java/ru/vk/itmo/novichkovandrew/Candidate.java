package ru.vk.itmo.novichkovandrew;

import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.iterator.TableIterator;

import java.util.Comparator;

public class Candidate<T> implements Comparable<Candidate<T>> {
    private Entry<T> entry;
    private final TableIterator<T> iterator;
    private final Comparator<T> comparator;

    public Candidate(TableIterator<T> iterator, Comparator<T> comparator) {
        this.iterator = iterator;
        this.comparator = comparator;
        this.entry = iterator.hasNext() ? iterator.next() : null;
    }

    public Entry<T> entry() {
        return entry;
    }

    public void update() {
        this.entry = iterator.hasNext() ? iterator.next() : null;
    }

    @Override
    public int compareTo(Candidate<T> candidate) {
        if (candidate.entry == null) {
            return -1;
        }
        if (entry == null) {
            return 1;
        }
        int memoryComparison = comparator.compare(this.entry.key(), candidate.entry.key());
        if (memoryComparison == 0) {
            return Integer.compare(candidate.iterator.getTableNumber(), this.iterator.getTableNumber());
        }
        return memoryComparison;
    }

    public boolean nonLast() {
        return this.entry != null;
    }

    public TableIterator<T> iterator() {
        return iterator;
    }
}
