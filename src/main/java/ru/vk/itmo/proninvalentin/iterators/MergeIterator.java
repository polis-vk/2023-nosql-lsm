package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.EnrichedEntry;
import ru.vk.itmo.proninvalentin.comparators.CreateAtTimeComparator;
import ru.vk.itmo.test.proninvalentin.DaoFactoryImpl;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class MergeIterator {
    private static boolean writeToConsole = false;

    // TODO: убрать
    private static void printCurrentEntries(List<EnrichedEntry> curItEntries) {
        var t = curItEntries.stream().map(c -> {
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
        System.out.println("Current entries state: " + t);
    }

    private static void printActualEntry(Entry<MemorySegment> actualEntry) {
        if (actualEntry == null) {
            System.out.println("Actual entry is NULL!");
        }
        var actualEntryKeyOutput = actualEntry == null || actualEntry.key() == null ? "Empty" : new DaoFactoryImpl().toString(actualEntry.key());
        var actualEntryValueOutput = actualEntry == null || actualEntry.value() == null ? "Empty" : new DaoFactoryImpl().toString(actualEntry.value());
        System.out.println("Actual entry [" + actualEntryKeyOutput + ":" + actualEntryValueOutput + "]");
    }

    public static Iterator<Entry<MemorySegment>> create(Iterator<Entry<MemorySegment>> memoryIterator,
                                                        List<Iterator<EnrichedEntry>> filesIterators,
                                                        Comparator<MemorySegment> msComparator) {
        List<Iterator<EnrichedEntry>> iterators = Stream
                .concat(Stream.of(EntryIteratorAdapter.create(memoryIterator)), filesIterators.stream())
                .filter(Iterator::hasNext)
                .toList();

        Comparator<EnrichedEntry> enrichedEntryComparator = getEnrichedEntryComparator(msComparator);
        // Двигаем значения итераторов, пропуская удаленные значения
        List<EnrichedEntry> curItEntries = new ArrayList<>(iterators.size());
        moveIteratorsNext(iterators, curItEntries);
        if (writeToConsole) {
            System.out.println("Init state");
            printCurrentEntries(curItEntries);
            var actualEntry = getActualEntry(curItEntries, enrichedEntryComparator);
            if (actualEntry != null) {
                printActualEntry(actualEntry.entry);
            }
        }
        moveIteratorsToTheNotDeletedEntry(iterators, curItEntries, enrichedEntryComparator, msComparator);
        if (writeToConsole) {
            System.out.println("End of moving to the first non deleted entry");
        }
        return new Iterator<>() {
            // Последнее отданное итераторами Entry
            Entry<MemorySegment> actualEntry;
            final Comparator<EnrichedEntry> comparator = enrichedEntryComparator;

            @Override
            public boolean hasNext() {
                // Проверяем есть ли хотя бы одно не удаленное значение
                return curItEntries.stream().anyMatch(Objects::nonNull);
            }

            // На каждом шаге мы задаем инвариант возвращаемой Entry:
            // Возвращаемая entry меньше всех остальных entry по ключу
            // Возвращаемая entry самая свежая по сравнению с entry с такими же ключами
            // Возвращаемая entry не удалена (metadata.isDeleted == false)
            @Override
            public Entry<MemorySegment> next() {
                actualEntry = getActualEntry(curItEntries, comparator).entry;
                if (writeToConsole) {
                    printActualEntry(actualEntry);
                }
                moveIteratorsWithSpecifiedEntry(iterators, curItEntries, actualEntry, msComparator);
                if (writeToConsole) {
                    printCurrentEntries(curItEntries);
                }
                moveIteratorsToTheNotDeletedEntry(iterators, curItEntries, enrichedEntryComparator, msComparator);
                return actualEntry;
            }
        };
    }

    // Двигаем наши итераторы до момента, когда хотя бы один итератор не будет обладать
    // наименьшим среди остальных и не удаленным Entry
    private static void moveIteratorsToTheNotDeletedEntry(List<Iterator<EnrichedEntry>> iterators,
                                                          List<EnrichedEntry> curItEntries,
                                                          Comparator<EnrichedEntry> enrichedEntryComparator,
                                                          Comparator<MemorySegment> msComparator) {
        EnrichedEntry actualEntry;
        actualEntry = getActualEntry(curItEntries, enrichedEntryComparator);
        while (actualEntry != null && actualEntry.metadata.isDeleted) {
            moveIteratorsWithSpecifiedEntry(iterators, curItEntries, actualEntry.entry, msComparator);
            actualEntry = getActualEntry(curItEntries, enrichedEntryComparator);
            System.out.println("Move next to get not deleted entry");
            printCurrentEntries(curItEntries);
        }
    }

    // Получить самую маленькую и свежую запись среди всех остальных записей
    private static EnrichedEntry getActualEntry(List<EnrichedEntry> curItEntries,
                                                Comparator<EnrichedEntry> comparator) {
        var entries = curItEntries.stream()
                .filter(Objects::nonNull)
                .sorted(comparator)
                .toList();
        if (entries.isEmpty()) {
            return null;
        }
        return entries.getFirst();
    }

    // Двигаем все итераторы вперед
    private static void moveIteratorsNext(List<Iterator<EnrichedEntry>> iterators,
                                          List<EnrichedEntry> curItEntries) {
        for (Iterator<EnrichedEntry> iterator : iterators) {
            curItEntries.add(iterator.next());
        }
    }

    // Двигаем вперед итераторы с указанным значением
    private static void moveIteratorsWithSpecifiedEntry(List<Iterator<EnrichedEntry>> iterators,
                                                        List<EnrichedEntry> curItEntries,
                                                        Entry<MemorySegment> entry,
                                                        Comparator<MemorySegment> msComparator) {
        if (writeToConsole) {
            System.out.println("Move iterators with key: " + new DaoFactoryImpl().toString(entry.key()));
        }
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<EnrichedEntry> curIt = iterators.get(i);
            EnrichedEntry curItEntry = curItEntries.get(i);
            boolean curItEntryEqualWithLastGivenEntry = curItEntry != null
                    && msComparator.compare(curItEntry.entry.key(), entry.key()) == 0;
            if (curItEntryEqualWithLastGivenEntry) {
                curItEntries.set(i, !curIt.hasNext() ? null : curIt.next());
            }
        }
    }

    private static Comparator<EnrichedEntry> getEnrichedEntryComparator(Comparator<MemorySegment> comparator) {
        // Наш компаратор сначало сортирует текущие значения итераторов по ключу Entry
        Comparator<EnrichedEntry> entryComparator = Comparator.comparing(p -> p.entry.key(), comparator);
        // Затем по времени создания Entry
        entryComparator = entryComparator.thenComparing(p -> p.metadata.createdAt, new CreateAtTimeComparator());
        return entryComparator;
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
