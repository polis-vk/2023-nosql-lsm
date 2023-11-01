package ru.vk.itmo.novichkovandrew.table;


import ru.vk.itmo.Entry;
import ru.vk.itmo.novichkovandrew.exceptions.FileChannelException;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


/**
 * Write table or tables into file.
 */
public class MMapTableWriter implements TableWriter {
    private final FileChannel channel;
    private final MemorySegment index;
    private final MemorySegment data;
    private final Handle indexHandle;
    private long indexOffset;
    private long dataOffset;
    private final Arena arena;


    public MMapTableWriter(Path path, Footer footer) throws IOException {
        this.channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );
        this.arena = Arena.ofShared();
        this.indexHandle = footer.getIndexHandle();
        this.dataOffset = 0L;
        this.indexOffset = 0L;
        this.data = channel.map(FileChannel.MapMode.READ_WRITE, 0L, indexHandle.offset(), arena); //todo: fix it to buffer;
        this.index = channel.map(FileChannel.MapMode.READ_WRITE, indexHandle.offset(), indexHandle.size(), arena);//todo: fix it to buffer;
    }

    @Override
    public void writeEntry(Entry<MemorySegment> entry) {
        dataOffset = copyToSegment(data, entry.key(), dataOffset);
        dataOffset = copyToSegment(data, entry.value(), dataOffset);
    }

    @Override
    public void writeIndexHandle(Entry<MemorySegment> entry) {
        long keyOffset = dataOffset;
        long valueOffset = keyOffset + entry.key().byteSize();
        valueOffset *= (entry.value() == null) ? -1 : 1;
        indexOffset = copyToSegment(index, keyOffset, indexOffset);
        indexOffset = copyToSegment(index, valueOffset, indexOffset);
    }

    @Override
    public void writeFooter(Footer footer) {
        finish(footer);
        try {
            MemorySegment segment = channel.map(FileChannel.MapMode.READ_WRITE, indexOffset, Footer.FOOTER_SIZE, arena);
            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, 0L, footer.getIndexHandle().offset());
            segment.set(ValueLayout.JAVA_LONG_UNALIGNED, Long.BYTES, footer.getIndexHandle().size());
        } catch (IOException e) {
            throw new FileChannelException("Couldn't map space for footer from index " + indexOffset, e);
        }

    }

    private void finish(Footer footer) {
        try {
            channel.position(dataOffset);
            if (indexHandle.offset() > dataOffset) {
                channel.transferTo(indexHandle.offset(), indexOffset, channel);
            }
        } catch (IOException e) {
            throw new FileChannelException(String.format("Couldn't move index to data in position %s", dataOffset), e);
        }
        footer.setIndexHandle(new Handle(dataOffset, indexOffset));
        indexOffset += dataOffset;
    }

    /**
     * Copy from one MemorySegment to another and return new offset in destination segment.
     */
    private long copyToSegment(MemorySegment dest, MemorySegment from, long offset) {
        MemorySegment source = from == null ? MemorySegment.NULL : from;
        MemorySegment.copy(source, 0, dest, offset, source.byteSize());
        return offset + source.byteSize();
    }

    /**
     * Copy long to MemorySegment and return new offset in destination segment.
     */
    private long copyToSegment(MemorySegment dest, long value, long offset) {
        dest.set(ValueLayout.JAVA_LONG_UNALIGNED, offset, value);
        return offset + Long.BYTES;
    }


    @Override
    public void close() throws IOException {
        if (arena.scope().isAlive()) {
            arena.close();
        }
        if (channel.isOpen()) {
            channel.truncate(indexOffset + Footer.FOOTER_SIZE);
        }
        channel.close();
    }
}
