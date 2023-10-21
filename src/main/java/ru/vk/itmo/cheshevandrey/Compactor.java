package ru.vk.itmo.cheshevandrey;

import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.Iterator;

public class Compactor {

    private final Path storagePath;

    public Compactor(Path storagePath) {
        this.storagePath = storagePath;
    }

    public void compact(Iterator<Entry<MemorySegment>> range) throws IOException {
        if (!range.hasNext()) {
            return;
        }

        DiskStorage.save(storagePath, new IteratorWrapper(range));
        DiskStorage.resetStateAfterCompact(storagePath);
    }

    private static final class IteratorWrapper implements Iterable<Entry<MemorySegment>> {

        private final Iterator<Entry<MemorySegment>> storageIterator;

        private IteratorWrapper(Iterator<Entry<MemorySegment>> storageIterator) {
            this.storageIterator = storageIterator;
        }

        @Override
        public Iterator<Entry<MemorySegment>> iterator() {
            return storageIterator;
        }
    }
}
