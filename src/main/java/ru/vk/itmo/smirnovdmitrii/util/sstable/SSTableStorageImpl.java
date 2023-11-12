package ru.vk.itmo.smirnovdmitrii.util.sstable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

public class SSTableStorageImpl implements SSTableStorage {
    private final ConcurrentLinkedDeque<SSTable> storage = new ConcurrentLinkedDeque<>();
    private final ExecutorService deleter = Executors.newSingleThreadExecutor();
    private final Path basePath;
    private final String indexFileName;
    private final AtomicBoolean isIndexChanging = new AtomicBoolean(false);

    public SSTableStorageImpl(final Path basePath, final String indexFileName) {
        this.basePath = basePath;
        this.indexFileName = indexFileName;
    }

    @Override
    public void add(final SSTable ssTable) throws IOException {
        if (ssTable == null) {
            return;
        }
        changeIndex(ssTable.path().getFileName().toString());
        storage.addFirst(ssTable);
    }

    @Override
    public void compact(final SSTable compaction, final List<SSTable> compacted) throws IOException {
        final List<String> fileNames = new ArrayList<>(compacted.size());
        final String compactionName = compaction == null ? null : compaction.path().getFileName().toString();
        for (final SSTable ssTable: compacted) {
            fileNames.add(ssTable.path().getFileName().toString());
        }
        changeIndex(compactionName, fileNames);
        storage.addLast(compaction);
        compacted.forEach(ssTable -> ssTable.isAlive().set(false));
        storage.removeAll(compacted);
        deleter.execute(() -> deleteTask(compacted));
    }

    private void deleteTask(final List<SSTable> compacted) {
        int deleted = 0;
        while (deleted < compacted.size()) {
            for (int i = 0; i < compacted.size(); i++) {
                final SSTable ssTable = compacted.get(i);
                if (ssTable == null || ssTable.readers().get() != 0) {
                    continue;
                }
                try {
                    Files.delete(ssTable.path());
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
                compacted.set(i, null);
                deleted++;
            }
        }
    }

    private void changeIndex(final String addFirst) throws IOException {
        changeIndex(l -> {
            final List<String> newIndex = new ArrayList<>();
            newIndex.add(addFirst);
            newIndex.addAll(l);
            return newIndex;
        });
    }

    private void changeIndex(final String addLast, final List<String> delete) throws IOException {
        changeIndex(l -> {
            final List<String> newIndex = new ArrayList<>(l);
            newIndex.removeAll(delete);
            if (addLast != null) {
                newIndex.add(addLast);
            }
            return newIndex;
        });
    }

    private void changeIndex(UnaryOperator<List<String>> transformer) throws IOException {
        final Path indexFilePath = basePath.resolve(indexFileName);
        final Path indexFilePathTmp = basePath.resolve(indexFilePath + ".tmp");
        while (true) {
            final List<String> newIndex = transformer.apply(Files.readAllLines(indexFilePath));
            Files.write(indexFilePathTmp, newIndex);
            if (isIndexChanging.compareAndSet(false, true)) {
                Files.move(indexFilePathTmp, indexFilePath,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                isIndexChanging.set(false);
                return;
            }
        }
    }

    @Override
    public void close() {
        deleter.close();
    }

    @Override
    public Iterator<SSTable> iterator() {
        return storage.iterator();
    }
}
