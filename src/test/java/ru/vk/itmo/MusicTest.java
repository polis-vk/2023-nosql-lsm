package ru.vk.itmo;


import ru.vk.itmo.test.DaoFactory;

import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MusicTest extends BaseTest {
    private static final char DELIMITER = '\0';
    private static final char DELIMITER_FOR_SUFFIX = DELIMITER + 1;

    private static String artistFrom(String artist) {
        assertEquals(-1, artist.indexOf(DELIMITER));
        assertEquals(-1, artist.indexOf(DELIMITER_FOR_SUFFIX));

        return artist;
    }

    private static String albumFrom(String artist, String album) {
        assertEquals(-1, artist.indexOf(DELIMITER));
        assertEquals(-1, artist.indexOf(DELIMITER_FOR_SUFFIX));
        assertEquals(-1, album.indexOf(DELIMITER));
        assertEquals(-1, album.indexOf(DELIMITER_FOR_SUFFIX));

        return artist + DELIMITER + album;
    }

    private static String trackFrom(
            String artist,
            String album,
            String track
    ) {
        assertEquals(-1, artist.indexOf(DELIMITER));
        assertEquals(-1, artist.indexOf(DELIMITER_FOR_SUFFIX));
        assertEquals(-1, album.indexOf(DELIMITER));
        assertEquals(-1, album.indexOf(DELIMITER_FOR_SUFFIX));
        assertEquals(-1, track.indexOf(DELIMITER));
        assertEquals(-1, track.indexOf(DELIMITER_FOR_SUFFIX));

        return artist + DELIMITER + album + DELIMITER + track;
    }

    private static Entry<String> record(String track, int duration) {
        return new BaseEntry<>(
                track,
                duration(duration)
        );
    }

    private static String duration(int seconds) {
        return Integer.toString(seconds);
    }

    @DaoTest(stage = 3)
    public void database(Dao<String, Entry<String>> dao) throws Exception {
        // Fill the music database
        dao.upsert(record(trackFrom("Ar1", "Al11", "T111"), 15));
        dao.upsert(record(trackFrom("Ar1", "Al11", "T112"), 24));
        dao.upsert(record(trackFrom("Ar1", "Al12", "T111"), 33));
        dao.upsert(record(trackFrom("Ar1", "Al12", "T1111"), 49));
        dao.upsert(record(trackFrom("Ar1", "Al12", "T112"), 50));
        dao.upsert(record(trackFrom("Ar2", "Al21", "T211"), 62));
        dao.upsert(record(trackFrom("Ar2", "Al21", "T212"), 78));

        // Re-open the music database
        dao.close();
        dao = DaoFactory.Factory.reopen(dao);

        // Artists
        assertRangeSize(dao, artistFrom("Ar1"), 5);
        assertRangeSize(dao, artistFrom("Ar2"), 2);

        // Albums
        assertRangeSize(dao, albumFrom("Ar1", "Al11"), 2);
        assertRangeSize(dao, albumFrom("Ar1", "Al12"), 3);
        assertRangeSize(dao, albumFrom("Ar2", "Al21"), 2);
    }

    private void assertRangeSize(
            Dao<String, Entry<String>> dao,
            String suffix,
            int count) throws Exception {
        Iterator<Entry<String>> range = dao.get(
                suffix + DELIMITER,
                suffix + DELIMITER_FOR_SUFFIX
        );

        int size = 0;
        while (range.hasNext()) {
            size++;
            range.next();
        }

        assertEquals(count, size);
    }
}
