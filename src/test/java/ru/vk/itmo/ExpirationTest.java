package ru.vk.itmo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;
import ru.vk.itmo.solnyshkoksenia.DaoImpl;

import java.io.File;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
public class ExpirationTest {
    private final static Config config = new Config(Path.of("var"), 100000);

    @Test
    void testSimple() throws IOException, InterruptedException {
        try (DaoImpl dao = new DaoImpl(config)) {
            for (int i = 0; i < 25; i++) {
                dao.upsert(entry(i), 1L);
            }
            Thread.sleep(1L);
            Assertions.assertIterableEquals(Collections.emptyList(), list(dao.all()));
        } finally {
            deleteDirectory(config.basePath().toFile());
        }
    }

    @Test
    void testLast() throws IOException, InterruptedException {
        try (DaoImpl dao = new DaoImpl(config)) {
            for (int i = 0; i < 25; i++) {
                dao.upsert(entry(i), 1L);
            }
            dao.upsert(entry(26), 1000 * 60L);
            Thread.sleep(1L);
            assertSameList(
                    dao.all(),
                    List.of(entry(26))
            );
        } finally {
            deleteDirectory(config.basePath().toFile());
        }
    }

    @Test
    void testLastMany() throws IOException {
        try (DaoImpl dao = new DaoImpl(config)) {
            List<Entry<MemorySegment>> values = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                dao.upsert(entry(i), 1000 * 60L);
                values.add(entry(i));
            }
            assertSameList(
                    dao.all(),
                    values
            );
        } finally {
            deleteDirectory(config.basePath().toFile());
        }
    }

    @Test
    void testWithTimeout() throws IOException {
        try (DaoImpl dao = new DaoImpl(config)) {
            List<Entry<MemorySegment>> values = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                dao.upsert(entry(i), 1000 * 60L);
                values.add(entry(i));
            }
            assertSameList(
                    dao.all(),
                    values
            );
        } finally {
            deleteDirectory(config.basePath().toFile());
        }
    }

    @Test
    void testAfterFlush() throws IOException {
        try (DaoImpl dao = new DaoImpl(config)) {
            List<Entry<MemorySegment>> values = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                dao.upsert(entry(i), 1000 * 60L);
                values.add(entry(i));
            }
            dao.flush();
            assertSameList(
                    dao.all(),
                    values
            );
        } finally {
            deleteDirectory(config.basePath().toFile());
        }
    }

    @Test
    void testAfterClose() throws IOException {
        try {
            DaoImpl dao = new DaoImpl(config);
            List<Entry<MemorySegment>> values = new ArrayList<>();
            for (int i = 0; i < 25; i++) {
                dao.upsert(entry(i), 1000 * 60L);
                values.add(entry(i));
            }
            dao.close();
            dao = new DaoImpl(config);
            assertSameList(
                    dao.all(),
                    values
            );
            dao.close();
        } finally {
            deleteDirectory(config.basePath().toFile());
        }
    }

    boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public void assertSameList(Iterator<Entry<MemorySegment>> iterator, List<Entry<MemorySegment>> expected) { // todo compare as strings
        int index = 0;
        for (Entry<MemorySegment> entry : expected) {
            if (!iterator.hasNext()) {
                throw new AssertionFailedError("No more entries in iterator: " + index + " from " + expected.size() + " entries iterated");
            }
            int finalIndex = index;
            Entry<MemorySegment> triple = iterator.next();
            Entry<String> entry1 = new BaseEntry<>(toString(triple.key()), toString(triple.value()));
            Entry<String> expectedEntry = new BaseEntry<>(toString(entry.key()), toString(entry.value()));
            Assertions.assertEquals(expectedEntry, entry1, () -> "wrong entry at index " + finalIndex + " from " + expected.size());
            index++;
        }
        if (iterator.hasNext()) {
            throw new AssertionFailedError("Unexpected entry at index " + index + " from " + expected.size() + " elements: " + iterator.next());
        }
    }

    public <T> List<T> list(Iterator<T> iterator) {
        List<T> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    public static Entry<MemorySegment> entry(int index) {
        return new BaseEntry<>(key(index), value(index));
    }

    public static MemorySegment key(int index) {
        return key("k", index);
    }

    public static MemorySegment value(int index) {
        return value("v", index);
    }

    public static MemorySegment key(String prefix, int index) {
        String paddedIdx = String.format("%010d", index);
        return fromString(prefix + paddedIdx);
    }

    public static MemorySegment value(String prefix, int index) {
        String paddedIdx = String.format("%010d", index);
        return fromString(prefix + paddedIdx);
    }

    public static MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(UTF_8));
    }

    public String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null : new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), UTF_8);
    }
}
