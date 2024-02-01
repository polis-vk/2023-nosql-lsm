package ru.vk.itmo.reference;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.*;

/**
 * Merges entry {@link Iterator}s.
 *
 * @author incubos
 */
final class MergingEntryIterator implements Iterator<TimeStampEntry> {
    private final List<WeightedPeekingEntryIterator> iterators;

    MergingEntryIterator(final List<WeightedPeekingEntryIterator> iterators) {
        assert iterators.stream().allMatch(WeightedPeekingEntryIterator::hasNext);

        this.iterators = new ArrayList<>(iterators);
    }

    @Override
    public boolean hasNext() {
        return !iterators.isEmpty();
    }

    @Override
    public TimeStampEntry next() {
        TimeStampEntry nextElement = null;

        iterators.removeIf(currentIterator -> !currentIterator.hasNext());

        int numberOfIterator = 0;
        int counter = 0;

        for(WeightedPeekingEntryIterator iterator: iterators) {
            if (nextElement == null) {
                nextElement = iterator.peek();
            } else {
                TimeStampEntry nextElementCandidate = iterator.peek();
                if (MemorySegmentComparator.INSTANCE.compare(nextElement.key(), nextElementCandidate.key()) > 0) {
                    nextElement = nextElementCandidate;
                    numberOfIterator = counter;
                } else if (MemorySegmentComparator.INSTANCE.compare(nextElement.key(), nextElementCandidate.key()) == 0) {
                    if(nextElementCandidate.timeStamp() > nextElement.timeStamp()) {
                        nextElement = nextElementCandidate;
                        iterators.get(numberOfIterator).next();
                        numberOfIterator = counter;
                    } else {
                        iterators.get(counter).next();
                    }
                }
            }
            counter += 1;
        }

        nextElement = iterators.get(numberOfIterator).next();
        if (!iterators.get(numberOfIterator).hasNext()) {
            iterators.remove(numberOfIterator);
        }

        return nextElement;
    }
}
