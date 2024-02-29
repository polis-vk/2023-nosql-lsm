package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data set in various tables.
 *
 * @author incubos
 */
final class TableSet {
    final MemTable memTable;
    final AtomicLong memTableSize;
    // null or read-only
    final MemTable flushingTable;
    // From freshest to oldest
    final List<SSTable> ssTables;

    private TableSet(
            final MemTable memTable,
            final AtomicLong memTableSize,
            final MemTable flushingTable,
            final List<SSTable> ssTables) {
        this.memTable = memTable;
        this.memTableSize = memTableSize;
        this.flushingTable = flushingTable;
        this.ssTables = ssTables;
    }

    static TableSet from(final List<SSTable> ssTables) {
        return new TableSet(
                new MemTable(),
                new AtomicLong(),
                null,
                ssTables);
    }

    TableSet flushing() {
        if (memTable.isEmpty()) {
            throw new IllegalStateException("Nothing to flush");
        }

        if (flushingTable != null) {
            throw new IllegalStateException("Already flushing");
        }

        return new TableSet(
                new MemTable(),
                new AtomicLong(),
                memTable,
                ssTables);
    }

    TableSet flushed(final SSTable flushed) {
        final List<SSTable> newSSTables = new ArrayList<>(ssTables.size() + 1);
        newSSTables.add(flushed);
        newSSTables.addAll(ssTables);
        return new TableSet(
                memTable,
                memTableSize,
                null,
                newSSTables);
    }

    TableSet compacted(
            final Set<SSTable> replaced,
            final SSTable with) {
        final List<SSTable> newSsTables = new ArrayList<>(this.ssTables.size() + 1);

        // Keep not replaced SSTables
        for (final SSTable ssTable : this.ssTables) {
            if (!replaced.contains(ssTable)) {
                newSsTables.add(ssTable);
            }
        }

        // Logically the oldest one
        newSsTables.add(with);

        return new TableSet(
                memTable,
                memTableSize,
                flushingTable,
                newSsTables);
    }

    Iterator<EntryWithTimestamp<MemorySegment>> get(
            final MemorySegment from,
            final MemorySegment to) {
        final List<WeightedPeekingEntryIterator> iterators =
                new ArrayList<>(2 + ssTables.size());

        // MemTable goes first
        final Iterator<EntryWithTimestamp<MemorySegment>> memTableIterator =
                memTable.get(from, to);
        if (memTableIterator.hasNext()) {
            iterators.add(
                    new WeightedPeekingEntryIterator(memTableIterator));
        }

        // Then goes flushing
        if (flushingTable != null) {
            final Iterator<EntryWithTimestamp<MemorySegment>> flushingIterator =
                    flushingTable.get(from, to);
            if (flushingIterator.hasNext()) {
                iterators.add(
                        new WeightedPeekingEntryIterator(flushingIterator));
            }
        }

        // Then go all the SSTables
        for (int i = 0; i < ssTables.size(); i++) {
            final SSTable ssTable = ssTables.get(i);
            final Iterator<EntryWithTimestamp<MemorySegment>> ssTableIterator =
                    ssTable.get(from, to);
            if (ssTableIterator.hasNext()) {
                iterators.add(
                        new WeightedPeekingEntryIterator(ssTableIterator));
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
        EntryWithTimestamp<MemorySegment> result = memTable.get(key);
        if (result != null) {
            // Transform tombstone
            return swallowTombstoneWithTimestamp(result);
        }

        // Then check flushing
        if (flushingTable != null) {
            result = flushingTable.get(key);
            if (result != null) {
                // Transform tombstone
                return swallowTombstoneWithTimestamp(result);
            }
        }

        // At last check SSTables from freshest to oldest
        for (final SSTable ssTable : ssTables) {
            Entry<MemorySegment> resultFromSstable = ssTable.get(key);
            if (resultFromSstable != null) {
                // Transform tombstone
                return swallowTombstone(resultFromSstable);
            }
        }

        // Nothing found
        return null;
    }

    private static Entry<MemorySegment> swallowTombstone(final Entry<MemorySegment> entry) {
        return entry.value() == null ? null : entry;
    }

    private static Entry<MemorySegment> swallowTombstoneWithTimestamp(final EntryWithTimestamp<MemorySegment> entry) {
        if (entry.value() == null) return null;
        return new BaseEntry<>(entry.key(), entry.value());
    }

    EntryWithTimestamp<MemorySegment> upsert(final Entry<MemorySegment> entry) {
        return memTable.upsert(entry);
    }

    Iterator<EntryWithTimestamp<MemorySegment>> allSSTableEntries() {
        final List<WeightedPeekingEntryIterator> iterators =
                new ArrayList<>(ssTables.size());

        for (int i = 0; i < ssTables.size(); i++) {
            final SSTable ssTable = ssTables.get(i);
            final Iterator<EntryWithTimestamp<MemorySegment>> ssTableIterator =
                    ssTable.get(null, null);
            iterators.add(
                    new WeightedPeekingEntryIterator(ssTableIterator));
        }

        return new MergingEntryIterator(iterators);
    }
}
