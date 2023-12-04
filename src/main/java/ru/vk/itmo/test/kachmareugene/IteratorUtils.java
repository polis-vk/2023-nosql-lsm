package ru.vk.itmo.test.kachmareugene;

import ru.vk.itmo.Entry;

import java.lang.foreign.MemorySegment;
import java.util.SortedMap;

public class IteratorUtils {
    public static void insertNew(SortedMap<MemorySegment, SSTableRowInfo> mp,
                                 SSTablesController controller, SSTableRowInfo info, MemorySegment to) {
        Entry<MemorySegment> kv = controller.getRow(info);

        if (kv == null) {
            return;
        }

        if (!mp.containsKey(kv.key())) {
            mp.put(kv.key(), info);
            return;
        }
        SSTableRowInfo old = mp.get(kv.key());

        SSTableRowInfo oldInfo = old.ssTableInd > info.ssTableInd ? info : old;
        SSTableRowInfo newInfo = old.ssTableInd < info.ssTableInd ? info : old;

        mp.put(controller.getRow(newInfo).key(), newInfo);

        // tail recursion
        if (oldInfo.isReversedToIter) {
            insertNew(mp, controller, controller.getPrevInfo(oldInfo, to), to);
        } else {
            insertNew(mp, controller, controller.getNextInfo(oldInfo, to), to);
        }
    }
}
