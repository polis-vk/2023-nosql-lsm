package ru.vk.itmo.khodosovaelena;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*;

/**
 * Merges entry {@link Iterator}s.
 *
 * @author incubos
 */
final class MergingEntryIterator implements Iterator<EntryWithTimestamp<MemorySegment>> {
    private final Queue<WeightedPeekingEntryIterator> iterators;

    MergingEntryIterator(final List<WeightedPeekingEntryIterator> iterators) {
        assert iterators.stream().allMatch(WeightedPeekingEntryIterator::hasNext);

        this.iterators = new PriorityQueue<>(iterators); //todo comparator by 1st entry key -> timestamp
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public EntryWithTimestamp<MemorySegment> next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        final WeightedPeekingEntryIterator top = iterators.remove();
        final EntryWithTimestamp<MemorySegment> result = top.next();

        if (top.hasNext()) {
            // Not exhausted
            iterators.add(top);
        }

        // Remove older versions of the key
        while (true) {
            final WeightedPeekingEntryIterator iterator = iterators.peek();
            if (iterator == null) {
                // Nothing left
                break;
            }

            // Skip entries with the same key
            final EntryWithTimestamp<MemorySegment> entry = iterator.peek();
            if (MemorySegmentComparator.INSTANCE.compare(result.key(), entry.key()) != 0) {
                if (result.timestamp() != entry.timestamp())
                    // Reached another key
                    break;
            }

            // Drop
            iterators.remove();
            // Skip
            iterator.next();
            if (iterator.hasNext()) {
                // Not exhausted
                iterators.add(iterator);
            }
        }

        return result;
    }
}
