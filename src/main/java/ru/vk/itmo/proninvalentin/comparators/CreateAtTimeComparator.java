package ru.vk.itmo.proninvalentin.comparators;

import java.util.Comparator;

// Сортирует даты в порядке убывания
public class CreateAtTimeComparator implements Comparator<Long> {
    @Override
    public int compare(Long createTime1, Long createTime2) {
        long subtraction = createTime2 - createTime1;
        if (subtraction < 0) {
            return -1;
        } else if (subtraction > 0) {
            return 1;
        } else {
            return 0;
        }
    }
}
