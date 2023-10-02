package ru.vk.itmo.shishiginstepan;

import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemDaoImpl implements Dao<MemorySegment, Entry<MemorySegment>> {
    private final ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> memStorage = new ConcurrentSkipListMap<>(
            (o1, o2) -> {
                var mismatch = o1.mismatch(o2);
                if (mismatch == -1) {
                    return 0;
                }

                if (mismatch == o1.byteSize()) {
                    return -1;
                }

                if (mismatch == o2.byteSize()) {
                    return 1;
                }
                byte b1 = o1.get(ValueLayout.JAVA_BYTE, mismatch);
                byte b2 = o2.get(ValueLayout.JAVA_BYTE, mismatch);
                return Byte.compare(b1, b2);
            }
    );

    private final PersistentStorage pStorage;
    private final Path basePath;

    public InMemDaoImpl(Path basePath) {
        this.basePath = basePath;
        pStorage = new PersistentStorage(this.basePath);
    }

    public InMemDaoImpl(){
        this.basePath = Paths.get("./");
        pStorage = new PersistentStorage(this.basePath);
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        if (to == null && from == null) {
            return this.memStorage.values().iterator();
        } else if (to == null) {
            return this.memStorage.tailMap(from).sequencedValues().iterator();
        } else if (from == null) {
            return this.memStorage.headMap(to).sequencedValues().iterator();
        } else {
            return this.memStorage.subMap(from, to).sequencedValues().iterator();
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var val = this.memStorage.get(key);
        if (val == null) {
            val = new Entry<MemorySegment>() {
                @Override
                public MemorySegment key() {
                    return key;
                }
                @Override
                public MemorySegment value() {
                    return pStorage.get(key);
                }
            } ;
        }
        return val;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        this.memStorage.put(entry.key(), entry);
    }


    @Override
    public void flush() {
        pStorage.store(memStorage.values());
        memStorage.clear();
    }
}

class PersistentStorage{
    // TODO заменить на интерфейс сстейбл
    private final SimpleSSTable sstable;
    PersistentStorage(Path basePath) {
        this.sstable = new SimpleSSTable(basePath);
    }

    public void store(Collection<Entry<MemorySegment>> data){

        final long[] dataSize = {0};
        data.forEach(x -> dataSize[0] += x.value().byteSize() + x.key().byteSize() + 16);
        sstable.writeEntries(data.iterator(), dataSize[0]);
    }

    public MemorySegment get(MemorySegment key) {
        return this.sstable.get(key);
    }
}

class SimpleSSTable{
    private final Path sstPath;
    private long size;
    SimpleSSTable(Path basePath){
        sstPath = Path.of(basePath.toAbsolutePath() + "/sstable");
        try {
            Files.createFile(sstPath);
        } catch (IOException e) {
            throw new RuntimeException(e);        }
    }

    public void writeEntries(Iterator<Entry<MemorySegment>> entries, long dataSize){
        this.size = dataSize;
        try (var fileChannel = FileChannel.open(sstPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            var file = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, dataSize, Arena.ofAuto());
            long j = 0;
            while (entries.hasNext()){
                var entry = entries.next();
                file.set(ValueLayout.JAVA_LONG_UNALIGNED, j,entry.key().byteSize());
                j += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                for (int i =0; i < entry.key().byteSize(); i++) {
                    file.set(ValueLayout.JAVA_BYTE, j, entry.key().getAtIndex(ValueLayout.JAVA_BYTE, i));
                    j++;
                }
                file.set(ValueLayout.JAVA_LONG_UNALIGNED, j,entry.value().byteSize());
                j += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                for (int i =0; i < entry.value().byteSize(); i++) {
                    file.set(ValueLayout.JAVA_BYTE, j, entry.value().getAtIndex(ValueLayout.JAVA_BYTE, i));
                    j++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public MemorySegment get(MemorySegment key) {
        try (var fileChannel = FileChannel.open(sstPath, StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            var file = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, this.size, Arena.ofAuto());
            long offset = 0;
            while (offset < this.size) {
                var keySize = file.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();

                if (-1 == MemorySegment.mismatch(key, 0, keySize, file, offset, offset+keySize)){
                    offset += keySize;
                    var valSize = file.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                    offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
                    return file.asSlice(offset, valSize);
                }
                offset += keySize;
                offset += file.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
                offset += ValueLayout.JAVA_LONG_UNALIGNED.byteSize();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }


}

