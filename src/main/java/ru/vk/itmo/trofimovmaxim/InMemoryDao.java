package ru.vk.itmo.trofimovmaxim;

import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;
    private Config config = null;
    private MemorySegment sstable;

    private static final Comparator<MemorySegment> COMPARE_SEGMENT = (o1, o2) -> {
        if (o1 == null || o2 == null) {
            return o1 == null ? -1 : 1;
        }

        long mism = o1.mismatch(o2);
        if (mism == -1) {
            return (int) (o1.byteSize() - o2.byteSize());
        }
        if (mism == o1.byteSize() || mism == o2.byteSize()) {
            return mism == o1.byteSize() ? -1 : 1;
        }
        return Byte.compare(
                o1.get(ValueLayout.OfByte.JAVA_BYTE, mism),
                o2.get(ValueLayout.OfByte.JAVA_BYTE, mism)
        );
    };

    public InMemoryDao() {
        memTable = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);
    }

    public InMemoryDao(Config config) {
        this.config = config;
        memTable = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(
                Files.walk(config.basePath()).findFirst().get().toFile(), "r"
        );
             FileChannel channel = randomAccessFile.getChannel()) {
            sstable = channel.map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length(), Arena.global());
        } catch (FileNotFoundException e) {
            System.err.println("File not found: " + e);
        } catch (IOException e) {
            System.err.println("IOException: " + e);
        } catch (Exception e) {
            System.err.println("Some exception: " + e);
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (from == null || to == null) {
            if (from == null && to == null) {
                return memTable.values().iterator();
            } else if (from == null) {
                return memTable.headMap(to).values().iterator();
            } else {
                return memTable.tailMap(from).values().iterator();
            }
        } else {
            return memTable.subMap(from, to).values().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var resultMemTable = memTable.get(key);
        if (resultMemTable != null) {
            return resultMemTable;
        }
        return null;

        // TODO 4bytes показывающие сколько байт занимает следующий key+value
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (config != null) {
            // TODO
        } else {
            System.err.println("DAO don't know path to save directory");
        }
    }
}
