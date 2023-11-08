package ru.vk.itmo.abramovilya;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

// TODO: Написать javadoc на английском
/**
 * Обертка над ConcurrentSkipListMap
 * Нужна чтобы динамически считать размер всех MemorySegment, лежащих внутри Map
 * memorySize может выдавать значения больше настоящего размера
 * Например, если сделать put(k1, v1), put(k1, v1), memorySize будет равен 2 * (k1.byteSize() + v1.byteSize())
 * Это нужно для того постоянно не брать блокировки; реализация рассчитывает на то что пользователь не очень часто
 * пишет значения с одним и тем же ключом
 */

public class ConcurrentMapWithSize extends ConcurrentSkipListMap<MemorySegment, Entry<MemorySegment>> {
    private final AtomicLong size = new AtomicLong(0);

    public ConcurrentMapWithSize(Comparator<? super MemorySegment> comparator) {
        super(comparator);
    }

    @Override
    public ru.vk.itmo.Entry<MemorySegment> put(MemorySegment key, ru.vk.itmo.Entry<MemorySegment> value) {
        long sizeToAdd = key.byteSize();
        if (value.value() != null) {
            sizeToAdd += value.value().byteSize();
        }
        size.addAndGet(sizeToAdd);
        return super.put(key, value);
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean remove(Object key, Object value) {
        boolean isRemoved = super.remove(key, value);
        if (isRemoved) {
            long sizeToAdd = -((MemorySegment) key).byteSize();
            MemorySegment valueMs = ((ru.vk.itmo.Entry<MemorySegment>) value).value();
            if (valueMs != null) {
                sizeToAdd -= valueMs.byteSize();
            }
            size.addAndGet(sizeToAdd);
        }
        return isRemoved;
    }

    public long memorySize() {
        return size.get();
    }
}
