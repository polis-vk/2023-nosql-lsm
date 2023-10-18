package ru.vk.itmo.test.ryabovvadim;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class GatheringIteratorWithPriority<T> implements FutureIterator<T> {
    private final FutureIterator<T> delegate;

    public GatheringIteratorWithPriority(Collection<FutureIterator<T>> iterators, Comparator<? super T> comparator) {
        this.delegate = new LazyIterator<>(
            () -> {
                FutureIterator<T> minIterator = null;
                for (var iterator : iterators) {
                    if (!iterator.hasNext()) {
                        continue;
                    }
                    
                    if (minIterator == null) {
                        minIterator = iterator;
                    } else if (comparator.compare(minIterator.showNext(), iterator.showNext()) > 0) {
                        minIterator = iterator;
                    }
                }
                
                if (minIterator == null) {
                    throw new NoSuchElementException();
                }
                for (var iterator : iterators) {
                    if (!iterator.hasNext() || iterator == minIterator) {
                        continue;
                    }
                    if (comparator.compare(minIterator.showNext(), iterator.showNext()) == 0) {
                        iterator.next();                        
                    }
                }

                return minIterator.next();
            },
            () -> iterators.stream().anyMatch(Iterator::hasNext)
        );
    }

    @Override
    public T showNext() {
        return delegate.showNext();
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public T next() {
        return delegate.next();
    }
}
