package ru.vk.itmo.proninvalentin.iterators;

import ru.vk.itmo.Entry;
import ru.vk.itmo.proninvalentin.EnrichedEntry;
import ru.vk.itmo.proninvalentin.comparators.CreateAtTimeComparator;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class MergeIterator {
    private MergeIterator() {
    }

    public static Iterator<Entry<MemorySegment>> create(Iterator<Entry<MemorySegment>> memoryIterator,
                                                        List<Iterator<EnrichedEntry>> filesIterators,
                                                        Comparator<MemorySegment> msComparator) {
        List<Iterator<EnrichedEntry>> iterators = Stream
                .concat(Stream.of(EntryIteratorAdapter.create(memoryIterator)), filesIterators.stream())
                .filter(Iterator::hasNext)
                .toList();

        Comparator<EnrichedEntry> enrichedEntryComparator = getEnrichedEntryComparator(msComparator);
        List<EnrichedEntry> curItEntries = new ArrayList<>(iterators.size());
        moveIteratorsNext(iterators, curItEntries);
        moveIteratorsToTheNotDeletedEntry(iterators, curItEntries, enrichedEntryComparator, msComparator);
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
                actualEntry = Objects.requireNonNull(getActualEntry(curItEntries, comparator)).entry;
                moveIteratorsWithSpecifiedEntry(iterators, curItEntries, actualEntry, msComparator);
                moveIteratorsToTheNotDeletedEntry(iterators, curItEntries, enrichedEntryComparator, msComparator);
                return actualEntry;
            }
        };
    }

    // Двигаем наши итераторы до момента, когда хотя бы один итератор не будет обладать актуальным Entry
    private static void moveIteratorsToTheNotDeletedEntry(List<Iterator<EnrichedEntry>> iterators,
                                                          List<EnrichedEntry> curItEntries,
                                                          Comparator<EnrichedEntry> enrichedEntryComparator,
                                                          Comparator<MemorySegment> msComparator) {
        EnrichedEntry actualEntry = getActualEntry(curItEntries, enrichedEntryComparator);
        while (actualEntry != null && actualEntry.metadata.isDeleted) {
            moveIteratorsWithSpecifiedEntry(iterators, curItEntries, actualEntry.entry, msComparator);
            actualEntry = getActualEntry(curItEntries, enrichedEntryComparator);
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
        for (int i = 0; i < iterators.size(); i++) {
            Iterator<EnrichedEntry> curIt = iterators.get(i);
            EnrichedEntry curItEntry = curItEntries.get(i);
            boolean curItEntryEqualWithLastGivenEntry = curItEntry != null
                    && msComparator.compare(curItEntry.entry.key(), entry.key()) == 0;
            if (curItEntryEqualWithLastGivenEntry) {
                curItEntries.set(i, null);
                if (curIt.hasNext()) {
                    curItEntries.set(i, curIt.next());
                }
            }
        }
    }

    private static Comparator<EnrichedEntry> getEnrichedEntryComparator(Comparator<MemorySegment> comparator) {
        // Наш компаратор сначала сортирует текущие значения итераторов по ключу Entry
        Comparator<EnrichedEntry> entryComparator = Comparator.comparing(p -> p.entry.key(), comparator);
        // Затем по времени создания Entry
        entryComparator = entryComparator.thenComparing(p -> p.metadata.createdAt, new CreateAtTimeComparator());
        return entryComparator;
    }
}
