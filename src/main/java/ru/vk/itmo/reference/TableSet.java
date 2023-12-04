package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Data set in various tables.
 *
 * @author incubos
 */
final class TableSet {
    final MemTable memTable;
    // From freshest to oldest
    final List<SSTable> ssTables;

    private TableSet(
            final MemTable memTable,
            final List<SSTable> ssTables) {
        this.memTable = memTable;
        this.ssTables = ssTables;
    }

    static TableSet from(final List<SSTable> ssTables) {
        return new TableSet(
                new MemTable(),
                ssTables);
    }

    int nextSequence() {
        return ssTables.stream()
                .mapToInt(t -> t.sequence)
                .max()
                .orElse(0) + 1;
    }

    TableSet flushed(final SSTable flushed) {
        final List<SSTable> newSSTables = new ArrayList<>(ssTables.size() + 1);
        newSSTables.add(flushed);
        newSSTables.addAll(ssTables);
        return TableSet.from(newSSTables);
    }

    TableSet compacted(final SSTable compacted) {
        return new TableSet(
                memTable,
                List.of(compacted));
    }

    Iterator<Entry<MemorySegment>> get(
            final MemorySegment from,
            final MemorySegment to) {
        final List<WeightedPeekingEntryIterator> iterators =
                new ArrayList<>(1 + ssTables.size());

        // MemTable goes first
        final Iterator<Entry<MemorySegment>> memTableIterator =
                memTable.get(from, to);
        if (memTableIterator.hasNext()) {
            iterators.add(
                    new WeightedPeekingEntryIterator(
                            Integer.MIN_VALUE,
                            memTableIterator));
        }

        // Then go all the SSTables
        for (int i = 0; i < ssTables.size(); i++) {
            final SSTable ssTable = ssTables.get(i);
            final Iterator<Entry<MemorySegment>> ssTableIterator =
                    ssTable.get(from, to);
            if (ssTableIterator.hasNext()) {
                iterators.add(
                        new WeightedPeekingEntryIterator(
                                i,
                                ssTableIterator));
            }
        }

        return switch (iterators.size()) {
            case 0 -> Collections.emptyIterator();
            case 1 -> iterators.get(0);
            default -> new MergingEntryIterator(iterators);
        };
    }

    Entry<MemorySegment> get(final MemorySegment key) {
        // Slightly optimized version not to pollute the heap

        // First check MemTable
        Entry<MemorySegment> result = memTable.get(key);
        if (result != null) {
            // Transform tombstone
            return result.value() == null ? null : result;
        }

        // Then check SSTables from freshest to oldest
        for (final SSTable ssTable : ssTables) {
            result = ssTable.get(key);
            if (result != null) {
                // Transform tombstone
                return result.value() == null ? null : result;
            }
        }

        // Nothing found
        return null;
    }

    void upsert(final Entry<MemorySegment> entry) {
        memTable.upsert(entry);
    }

    Iterator<Entry<MemorySegment>> allDiskEntries() {
        final List<WeightedPeekingEntryIterator> iterators =
                new ArrayList<>(ssTables.size());

        for (int i = 0; i < ssTables.size(); i++) {
            final SSTable ssTable = ssTables.get(i);
            final Iterator<Entry<MemorySegment>> ssTableIterator =
                    ssTable.get(null, null);
            iterators.add(
                    new WeightedPeekingEntryIterator(
                            i,
                            ssTableIterator));
        }

        return new MergingEntryIterator(iterators);
    }
}
