package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Config;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class DaoWithCompaction extends InMemoryDao {
    private final ExecutorService flusherService;
    private final ExecutorService compactionService;
    private final ExecutorService fileDeleter;
    private final AtomicBoolean isFlushing;
    private final AtomicBoolean isClosedExecutors = new AtomicBoolean(false);
    private final AtomicLong numOfBiggestAliveFile;
    private final AtomicLong numOfSmallestAliveFile = new AtomicLong(-1);
    private final SortedMap<MemorySegment, Entry<MemorySegment>> mpBuffer =
            new ConcurrentSkipListMap<>(memorySegmentComparatorImpl);
    private AtomicReference<Future> compactFuture = new AtomicReference<>();
    public DaoWithCompaction() {
        super();
        this.isFlushing = null;
        this.flusherService = null;
        this.compactionService = null;
        this.fileDeleter = null;
        this.numOfBiggestAliveFile = controller.maximumFileNum;
    }

    public DaoWithCompaction(Config conf) {
        super(conf);
        this.flusherService = Executors.newSingleThreadExecutor();
        this.compactionService = Executors.newSingleThreadExecutor();
        this.fileDeleter = Executors.newSingleThreadExecutor();
        this.isFlushing = new AtomicBoolean(false);
        this.numOfBiggestAliveFile = controller.maximumFileNum;

        flusherService.execute(() -> {
            while (!isClosedExecutors.get()) {
                if (byteSize.get() >= conf.flushThresholdBytes()) {
                    byteSize.set(0L);
                    isFlushing.set(true);

                    mpBuffer.putAll(getMemTable());
                    getMemTable().clear();

                    try {
                        controller.addFileToLists(close(mpBuffer));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    isFlushing.set(false);
                }
            }
        });

        var ignore1 = fileDeleter.submit(() -> {
            try {
                while (!isClosedExecutors.get()) {
                    controller.tryToDelete();
                }
                controller.tryToDelete();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        ignore1.state();
    }

    @Override
    public void compact() throws IOException {

        compactFuture.set(compactionService.submit(() -> {
            try {
                Pair<Path, Long> res =
                        controller.dumpIterator(new SSTableIterable(new ArrayList<>(),
                                controller, null, null, numOfSmallestAliveFile.get()));
                controller.addFileToLists(res.first);

                numOfBiggestAliveFile.set(res.second);
                numOfSmallestAliveFile.set(res.second);
                controller.decreaseAllSmall(res.second - 1);
                controller.tryToDelete();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }));
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        if (isFlushing.get() && byteSize.get() >= conf.flushThresholdBytes()) {
            throw new OutOfMemoryError("Upserting while flush is running!");
        } else {
            super.upsert(entry);
            // 5242901
            // 1048576
            if (entry.value() == null) {
                System.out.println(entry.key().byteSize() + " " + conf.flushThresholdBytes());
            } else {
                System.out.println((entry.key().byteSize() + entry.value().byteSize()) + " " + conf.flushThresholdBytes());
            }
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new SSTableIterator(super.get(from, to), controller, from, to, numOfSmallestAliveFile.get());
    }

    @Override
    public void close() throws IOException {
        isClosedExecutors.set(true);
        flusherService.close();
        while (compactFuture.get() != null && !compactFuture.get().isDone()) {}

        compactionService.close();
        fileDeleter.close();

        super.close();
    }
}
