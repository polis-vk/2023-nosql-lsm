package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.EnrichedEntry;
import ru.vk.itmo.proninvalentin.comparators.CreateAtTimeComparator;
import ru.vk.itmo.test.proninvalentin.DaoFactoryImpl;

import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.stream.Stream;

public class MergeIterator {
    public static Iterator<Entry<MemorySegment>> create(Iterator<Entry<MemorySegment>> memoryIterator,
                                                        List<Iterator<EnrichedEntry>> filesIterators,
                                                        Comparator<MemorySegment> comparator) {
        List<Iterator<EnrichedEntry>> iterators = Stream
                .concat(Stream.of(EntryIteratorAdapter.create(memoryIterator)), filesIterators.stream())
                .filter(Iterator::hasNext)
                .toList();

        // Инициализируем текущий EnrichedEntry у каждого итератора
        List<EnrichedEntry> curItEntries = new ArrayList<>(iterators.size());
        for (int i = 0; i < iterators.size(); i++) {
            curItEntries.add(iterators.get(i).next());
        }

        // Наш компаратор сначало сортирует значения итераторов по ключу Entry
        Comparator<EnrichedEntry> pairComparator = Comparator.comparing(p -> p.entry.key(), comparator);
        // Затем по времени создания Entry
        Comparator<Long> createdAtTimeComparator = Comparator.comparing(p -> p, new CreateAtTimeComparator());
        pairComparator = pairComparator.thenComparing(p -> p.metadata.createdAt, createdAtTimeComparator);
        Comparator<EnrichedEntry> finalPairComparator = pairComparator;
        return new Iterator<>() {
            // Последнее отданное итератором Entry
            Entry<MemorySegment> lastGivenEntry;

            @Override
            public boolean hasNext() {
                // Проверяем есть ли хотя бы один итератор с hasNext или значением, которое мы не обработали
                return iterators.stream().anyMatch(Iterator::hasNext)
                        || curItEntries.stream().anyMatch(Objects::nonNull);
            }

            @Override
            public Entry<MemorySegment> next() {
                boolean writeToConsole = false;
                if (writeToConsole) {
                    var t = curItEntries.stream().filter(Objects::nonNull).sorted(finalPairComparator).map(c -> {
                        if (c == null || c.entry == null) {
                            return "Empty";
                        }
                        var ckey = c.entry.key() == null ? "Empty" : new DaoFactoryImpl().toString(c.entry.key());
                        var cvalue = c.entry.value() == null ? "Empty" : new DaoFactoryImpl().toString(c.entry.value());
                        if (c.metadata != null) {
                            var createdAt = c.metadata.createdAt;
                            var isDeleted = c.metadata.isDeleted;
                            return ckey + ":" + cvalue + ":" + createdAt + ":" + isDeleted;
                        }

                        return ckey + ":" + cvalue;
                    }).toList();
                    System.out.println(t);
//                    var bb = curItEntries.stream().filter(Objects::nonNull).sorted(finalPairComparator).toList();
//                    System.out.println(bb == null ? "--" : "__");
                }
                lastGivenEntry = curItEntries.stream().filter(Objects::nonNull).sorted(finalPairComparator).toList().getFirst().entry;
                if (writeToConsole) {
                    var lastGivenEntryKeyOutput = lastGivenEntry == null || lastGivenEntry.key() == null ? "Empty" : new DaoFactoryImpl().toString(lastGivenEntry.key());
                    System.out.println("Last given entry key: " + lastGivenEntryKeyOutput);
                    var lastGivenEntryValueOutput = lastGivenEntry == null || lastGivenEntry.value() == null ? "Empty" : new DaoFactoryImpl().toString(lastGivenEntry.value());
                    System.out.println("Last given entry value: " + lastGivenEntryValueOutput);
                }
                // Двигаем вперед если hasNext == true и текущее значение равно последнему найденному
                for (int i = 0; i < iterators.size(); i++) {
                    Iterator<EnrichedEntry> curIt = iterators.get(i);
                    EnrichedEntry curItEntry = curItEntries.get(i);
                    boolean curItEntryEqualWithLastGivenEntry = curItEntry != null && lastGivenEntry != null
                            && comparator.compare(curItEntry.entry.key(), lastGivenEntry.key()) == 0;
                    if (curItEntryEqualWithLastGivenEntry) {
                        curItEntries.set(i, !curIt.hasNext() ? null : curIt.next());
                    }
                }

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
