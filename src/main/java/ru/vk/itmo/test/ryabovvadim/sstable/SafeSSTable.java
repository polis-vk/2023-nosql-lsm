package ru.vk.itmo.test.ryabovvadim.sstable;

import ru.vk.itmo.Entry;
import ru.vk.itmo.test.ryabovvadim.iterators.FutureIterator;
import ru.vk.itmo.test.ryabovvadim.iterators.LazyIterator;
import ru.vk.itmo.test.ryabovvadim.utils.FileUtils;
import ru.vk.itmo.test.ryabovvadim.utils.IteratorUtils;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicInteger;

import static ru.vk.itmo.test.ryabovvadim.utils.FileUtils.DATA_FILE_EXT;
import static ru.vk.itmo.test.ryabovvadim.utils.FileUtils.DELETED_FILE_EXT;

public class SafeSSTable {
    private final SSTable ssTable;
    private final AtomicInteger countAliveRef = new AtomicInteger();

    public SafeSSTable(SSTable ssTable) {
        this.ssTable = ssTable;
    }

    public SSTable ssTable() {
        return ssTable;
    }

    public Entry<MemorySegment> findEntry(MemorySegment key) {
        if (incrementRef()) {
            return null;
        }

        Entry<MemorySegment> result = ssTable.findEntry(key);
        decrementRef();
        return result;
    }

    public FutureIterator<Entry<MemorySegment>> findEntries(MemorySegment from, MemorySegment to) {
        if (incrementRef()) {
            return IteratorUtils.emptyFutureIterator();
        }

        FutureIterator<Entry<MemorySegment>> iterator = ssTable.findEntries(from, to);
        if (!iterator.hasNext()) {
            return iterator;
        }

        return new LazyIterator<>(
                () -> {
                    Entry<MemorySegment> next = iterator.next();
                    if (!iterator.hasNext()) {
                        decrementRef();
                    }
                    return next;
                },
                iterator::hasNext
        );
    }

    public void delete(Path path) throws IOException {
        countAliveRef.getAndUpdate(x -> -(x + 1));
        Path dataFile = FileUtils.makePath(path, Long.toString(ssTable.getId()), DATA_FILE_EXT);
        Path deletedFile = FileUtils.makePath(path, Long.toString(ssTable.getId()), DELETED_FILE_EXT);
        Files.move(dataFile, deletedFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        synchronized (countAliveRef) {
            try {
                while (!Thread.interrupted() && countAliveRef.get() != -1) {
                    countAliveRef.wait();
                }
                Files.deleteIfExists(deletedFile);
            } catch (InterruptedException ignored) {
                // Ignored exception
            }
        }
    }

    private boolean incrementRef() {
        return countAliveRef.getAndUpdate(x -> x < 0 ? x : x + 1) < 0;
    }

    private void decrementRef() {
        if (countAliveRef.updateAndGet(x -> x < 0 ? x + 1 : x - 1) == -1) {
            synchronized (countAliveRef) {
                countAliveRef.notifyAll();
            }
        }
    }
}
