package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.test.proninvalentin.DaoFactoryImpl;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class MergeIterator {
    static class Pair {
        public final int index;
        public Entry<MemorySegment> value;

        public Pair(int index, Entry<MemorySegment> value) {
            this.index = index;
            this.value = value;
        }
    }

    public static Iterator<Entry<MemorySegment>> create(Iterator<Entry<MemorySegment>> memoryIterator,
                                                        List<Iterator<Entry<MemorySegment>>> filesIterators,
                                                        Comparator<MemorySegment> comparator) {
        List<Iterator<Entry<MemorySegment>>> iterators = Stream
                .concat(Stream.of(memoryIterator), filesIterators.stream())
                .filter(Iterator::hasNext)
                .toList();

        Comparator<Pair> pairComparator = Comparator.comparing(p -> p.value == null ? null : p.value.key(), comparator);
        pairComparator = pairComparator.thenComparing(p -> p.index);

        // Инициализируем текущий Entry у каждого итератора как null
        List<Pair> curItEntries = new ArrayList<>(iterators.size());
        for (int i = 0; i < iterators.size(); i++) {
            curItEntries.add(new Pair(i, null));
        }

        Comparator<Pair> finalPairComparator = pairComparator;
        return new Iterator<>() {
            // Последнее отданное Entry
            Entry<MemorySegment> lastGivenEntry;

            @Override
            public boolean hasNext() {
                // Проверяем есть ли хотя бы один итератор с hasNext или значение, которое мы не обработали
                return iterators.stream().anyMatch(Iterator::hasNext)
                        || curItEntries.stream().anyMatch(e -> e.value != null);
            }

            @Override
            public Entry<MemorySegment> next() {
                // Двигаем вперед если hasNext == true и текущее значение равно последнему найденному
                for (int i = 0; i < iterators.size(); i++) {
                    Iterator<Entry<MemorySegment>> it = iterators.get(i);
                    var curItEntry = curItEntries.get(i);
                    var curItEntryEqualWithLastGivenEntry = curItEntry.value != null
                            && comparator.compare(curItEntry.value.key(), lastGivenEntry.key()) == 0;
                    if (lastGivenEntry == null || curItEntryEqualWithLastGivenEntry) {
                        curItEntry.value = !it.hasNext() ? null : it.next();
                    }
                }
                var t = curItEntries.stream().map(c -> {
                    if (c.value == null) {
                        return "Empty";
                    }
                    return new DaoFactoryImpl().toString(c.value.key());
                }).toList();
                System.out.println(t);
                lastGivenEntry = curItEntries.stream().sorted(finalPairComparator).toList().getFirst().value;
                if (lastGivenEntry != null) {
                    for (var curItEntry : curItEntries) {
                        if (curItEntry != null && curItEntry.value != null
                                && comparator.compare(curItEntry.value.key(), lastGivenEntry.key()) == 0) {
                            curItEntry.value = null;
                        }
                    }
                }

                var lastGivenEntryOutput = lastGivenEntry == null || lastGivenEntry.key() == null ? "Empty" : new DaoFactoryImpl().toString(lastGivenEntry.key());
                System.out.println("Last given entry: " + lastGivenEntryOutput);

                return lastGivenEntry;
            }
        };
    }
    /*
     get(k2, k6)
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
