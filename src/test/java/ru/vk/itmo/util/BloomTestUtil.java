package ru.vk.itmo.util;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;
import ru.vk.itmo.bandurinvladislav.BloomFilter;
import ru.vk.itmo.bandurinvladislav.PersistentDao;

import javax.xml.stream.events.EntityReference;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class BloomTestUtil {

    private BloomTestUtil() {
    }

    @SuppressWarnings("unchecked")
    public static List<MemorySegment> getSegmentList(Dao<String, Entry<String>> dao) throws NoSuchFieldException, IllegalAccessException {
        Class<? extends Dao> daoClass = dao.getClass();
        Field daoField = daoClass.getDeclaredField("delegate");
        daoField.setAccessible(true);
        PersistentDao persistentDao = (PersistentDao) daoField.get(dao);

        Field stateField = PersistentDao.class.getDeclaredField("state");
        stateField.setAccessible(true);
        AtomicReference<Object> storageState = (AtomicReference<Object>) stateField.get(persistentDao);
        Field diskSegmentListField = storageState.get().getClass().getDeclaredField("diskSegmentList");
        storageState.get().getClass().getDeclaredConstructors()[0].setAccessible(true);
        diskSegmentListField.setAccessible(true);
        return (List<MemorySegment>) diskSegmentListField.get(storageState.get());
    }

    public static Entry<MemorySegment> stringToMemorySegmentEntry(Entry<String> entry) {
        BaseEntry<MemorySegment> e = new BaseEntry<>(
                fromString(entry.key()),
                fromString(entry.value())
        );
        return fromBaseEntry(e);
    }

    public static String toString(MemorySegment memorySegment) {
        return memorySegment == null ? null :
                new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    public static String toStringS(MemorySegment memorySegment) {
        return memorySegment == null ? null :
                new String(memorySegment.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    public static MemorySegment fromString(String data) {
        return data == null ? null : MemorySegment.ofArray(data.getBytes(StandardCharsets.UTF_8));
    }

    public static Entry<MemorySegment> fromBaseEntry(Entry<MemorySegment> baseEntry) {
        return baseEntry;
    }

}
