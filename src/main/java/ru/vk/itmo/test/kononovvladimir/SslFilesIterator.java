package ru.vk.itmo.test.kononovvladimir;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Entry;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

public class SslFilesIterator implements Iterator<Entry<MemorySegment>> {

    IndexSearcher indexSearcher;
    DataSearcher dataSearcher;
    KeySearcher keySearcher;

    MemorySegment indexSegment;
    MemorySegment dataSegment;
    MemorySegment keySegment;
    long toIndex;
    MemorySegment currentPeek = null;

    public SslFilesIterator(Path pathIndex, Path pathData, Path pathKey, long toIndex) throws IOException {
        Arena arena = Arena.ofShared();
        try (FileChannel fileChannel = FileChannel.open(pathIndex, StandardOpenOption.READ)){
            this.indexSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena);
            indexSearcher = new IndexSearcher(indexSegment);
        }
        long size = indexSearcher.getSslSize();
        try (FileChannel fileChannel = FileChannel.open(pathData, StandardOpenOption.READ)){
            this.dataSegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena);
            dataSearcher = new DataSearcher(dataSegment, size);
        }
        try (FileChannel fileChannel = FileChannel.open(pathKey, StandardOpenOption.READ)){
            this.keySegment = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size(), arena);
            keySearcher = new KeySearcher(keySegment, size);
        }
        this.toIndex = toIndex;
    }

    @Override
    public boolean hasNext() {
        if (peek != null) {
            return true;
        }
        else {
            if (dataSearcher.canContinue() && keySearcher.canContinue()) {
                peek = new BaseEntry<>(keySearcher.getValueInStrokeAndGo(), dataSearcher.getValueInStrokeAndGo());
            }
        }
        return peek != null; // Точно?
    }

    public void goToIndex(long index) {
       // currentOffsetIndex =
    }

    public void goToOffset(long dataOffset, long keyOffset){
        dataSearcher.goToOffset(dataOffset, 0);
        keySearcher.goToOffset(keyOffset, 0);
    }

    public Entry<MemorySegment> peek;

    public Entry<MemorySegment> getPeek(){
        if (!hasNext()) return null;
        return peek;
    }

    @Override
    public Entry<MemorySegment> next() {
        if (!hasNext()) return null;
/*        long dataOffset = indexSearcher.getDataOffset(currentIndex);
        long keyOffset = indexSearcher.getKeyOffset(currentIndex);

        long dataSize = dataSearcher.getLongAtOffset();
        long keySize = keySearcher.getLongAtOffset();*/
        Entry<MemorySegment> res = getPeek();
        peek = null;
        return res;
/*        if (lastInMemoryEntry == null){
            current++;
            return new BaseEntry<>(memorySegmentInFileKey, memorySegmentInFileData);
        }

        MemorySegment memorySegmentInMemoryKey = lastInMemoryEntry.key();
        long compare = memorySegmentComparator.compare(memorySegmentInMemoryKey, memorySegmentInFileKey);
        Entry<MemorySegment> result;
        if (compare <= 0){
            if (compare == 0) {
                current++;
            }
            result = lastInMemoryEntry;
            nextInMemoryEntry();
        } else {
            result = new BaseEntry<>(memorySegmentInFileKey, memorySegmentInFileData);
            current++;
        }
        return result;*/
    }
}
