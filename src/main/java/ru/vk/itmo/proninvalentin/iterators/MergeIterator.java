package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class MergeIterator {
    public static Iterator<Entry<MemorySegment>> create(Iterator<Entry<MemorySegment>> memoryIterator,
                                                        List<Iterator<Entry<MemorySegment>>> filesIterators,
                                                        Comparator<MemorySegment> comparator) {
        return new Iterator<>() {

            Entry<MemorySegment> lastEntry;
            // Последнее Entry итератора для памяти
            Entry<MemorySegment> lastMemoryItEntry;
            // Список последних Entry у каждолго итератора для файла
            final List<Entry<MemorySegment>> lastFilesItEntry = new ArrayList<>(filesIterators.size());

            @Override
            public boolean hasNext() {
                // Проверяем есть ли хотя бы один итератор с hasNext

                return memoryIterator.hasNext() /*|| filesIterators.stream().anyMatch(Iterator::hasNext)*/;
            }

            @Override
            public Entry<MemorySegment> next() {
                /*if (memoryIterator.hasNext()) {
                    lastMemoryItEntry = memoryIterator.next();
                }

                for (int i = 0; i < lastFilesItEntry.size(); i++) {
                    if (comparator.compare(lastFilesItEntry.get(i).key(), lastMemoryItEntry.key()) == 0) {
                        if (filesIterators.get(i).hasNext()) {
                            lastFilesItEntry.set(i, filesIterators.get(i).next());
                        }
                    }
                }*/
                // По алгоритму берем нужный нам итератор
                return memoryIterator.next();
            }
        };
    }

    /*
     Пример: у нас есть три файла со следующим содержимым
     |k1 k2| |k0 k2 k4| |k1 k3|
     И данные в буфере
     |k2 k5|

     Создаем 4 итератора, по одному на каждый файл и один на буфер
     Теперь двигаем каждый итератор к максимальному элементу больше from или ровно на from в файле
     Шаг алгоритма:
     1)
     Номер итератора в файле: ключ
     1: k1
     2: k2
     3: k1
     4: k2

     Сортируем значения итераторов между собой по значению, а потом по номеру итератора
     Сортированные значения:
     (3)k1 (1)k1 (4)k2 (2)k2
     Возвращаем первый минимальный ключ у максимального итератора
     Результирующий итератор: (3)k1
     2)
     Двигаем итераторы c найденным значением вправо
     1: k1 -> k2
     2: k2
     3: k1 -> k3
     4: k2
     Опять сортируем по значению и находим итератор более свежего файла
     (4)k2 (2)k2 (1)k2 (3)k3
     Понимаем, что нам нужен (4)k2
     Результирующий итератор: (3)k1 (4)k2
     3)
     Двигаем итераторы c найденным значением вправо
     1: k2 -> end
     2: k2 -> k4
     3: k3
     4: k2 -> k5
     Опять сортируем по значению и находим итератор более свежего файла
     (3)k3 (2)k4 (4)k5
     Понимаем, что нам нужен (3)k3
     Результирующий итератор: (3)k1 (4)k2 (3)k3
     4)
     Двигаем итераторы c найденным значением вправо
     1: end
     2: k4
     3: k3 -> end
     4: k5
     Опять сортируем по значению и находим итератор более свежего файла
     (2)k4 (4)k5
     Понимаем, что нам нужен (2)k4
     Результирующий итератор: (3)k1 (4)k2 (3)k3 (2)k4
     5)
     Двигаем итераторы c найденным значением вправо
     1: end
     2: k4 -> end
     3: end
     4: k5
     Опять сортируем по значению и находим итератор более свежего файла
     (4)k5
     Понимаем, что нам нужен (4)k5
     Результирующий итератор: (3)k1 (4)k2 (3)k3 (2)k4 (4)k5
     6)
     Двигаем итераторы c найденным значением вправо
     1: end
     2: end
     3: end
     4: k5 -> end
     Понимаем, что все итераторы дошли до конца, ничего не возвращаем
     Результирующий итератор: (3)k1 (4)k2 (3)k3 (2)k4 (4)k5
    */
}
