package ru.vk.itmo.trofimovmaxim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final String FILENAME = "sstable";

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;
    private Config config;
    private SsTable ssTable;

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
        ssTable = new SsTable(FILENAME);
        if (ssTable.data == null || ssTable.offsetsTable == null) {
            ssTable = null;
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
        if (ssTable != null) {
            return ssTable.get(key);
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    @Override
    public void close() throws IOException {
        if (config == null) {
            return;
        }
        ArrayList<SsTable.Offset> offsets = new ArrayList<>();
        try (FileOutputStream stream = new FileOutputStream(
                config.basePath().resolve(FILENAME + ".data").toFile());
             FileOutputStream outputStream = new FileOutputStream(
                     config.basePath().resolve(FILENAME + ".meta").toFile());
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {

            for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : memTable.entrySet()) {
                MemorySegment key = entry.getKey();
                Entry<MemorySegment> val = entry.getValue();

                long prevOffset = 0;
                if (!offsets.isEmpty()) {
                    var last = offsets.getLast();
                    prevOffset = last.keyOffset + last.keySize + last.val1Size + last.val2Size;
                }

                stream.write(toBytes(key));
                stream.write(toBytes(val.key()));
                stream.write(toBytes(val.value()));

                offsets.add(
                        new SsTable.Offset(prevOffset, key.byteSize(), val.key().byteSize(), val.value().byteSize()));
            }

            objectOutputStream.writeObject(offsets);
        }
    }

    private byte[] toBytes(MemorySegment segm) {
        return segm.toArray(ValueLayout.OfByte.JAVA_BYTE);
    }

    private class SsTable {
        MemorySegment data;
        List<Offset> offsetsTable;

        SsTable(String filename) {
            Path pathToSsTable = config.basePath().resolve(filename + ".data");
            Path pathToOffsetsTable = config.basePath().resolve(filename + ".meta");

            try (RandomAccessFile randomAccessFile = new RandomAccessFile(pathToSsTable.toFile(), "r");
                 FileChannel channel = randomAccessFile.getChannel();
                 FileInputStream fileInputStream = new FileInputStream(pathToOffsetsTable.toFile());
                 ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {

                data = channel.map(FileChannel.MapMode.READ_ONLY, 0, randomAccessFile.length(), Arena.global());
                offsetsTable = (ArrayList<Offset>) objectInputStream.readObject();
            } catch (Exception ignored) {
            }
        }

        Entry<MemorySegment> get(MemorySegment key) {
            for (Offset offset : offsetsTable) {
                MemorySegment currentKey = MemorySegment.ofArray(read(offset.keyOffset, offset.keySize));
                if (COMPARE_SEGMENT.compare(currentKey, key) == 0) {
                    return new BaseEntry<>(
                            MemorySegment.ofArray(read(offset.keyOffset + offset.keySize, offset.val1Size)),
                            MemorySegment.ofArray(
                                    read(offset.keyOffset + offset.keySize + offset.val1Size, offset.val2Size))
                    );
                }
            }
            return null;
        }

        byte[] read(long offset, long size) {
            byte[] result = new byte[(int) size];
            for (int i = 0; i < size; ++i) {
                result[i] = data.get(ValueLayout.OfByte.JAVA_BYTE, offset + i);
            }
            return result;
        }

        static class Offset implements Serializable {
            long keyOffset;
            long keySize;
            long val1Size;
            long val2Size;

            public Offset(long keyOffset, long keySize, long val1Size, long val2Size) {
                this.keyOffset = keyOffset;
                this.keySize = keySize;
                this.val1Size = val1Size;
                this.val2Size = val2Size;
            }
        }
    }
}
