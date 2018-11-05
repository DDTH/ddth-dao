package com.github.ddth.dao.qnd.nosql.cassandra;

import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.dao.nosql.cassandra.CassandraKdWideColumnStorage;

public class QndCassandraKdWideColumnStorage extends BaseNosqlCassandraQnd {
    public static void main(String[] args) throws Exception {
        Random random = new Random(System.currentTimeMillis());

        try (SessionManager sm = sessionManager()) {
            String KEYSPACE = "test";
            sm.execute("CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE
                    + " WITH REPLICATION={'class' : 'SimpleStrategy', 'replication_factor' : 1}");

            String TABLE = "tbl_kbwide", COL_KEY = "key", COL_FIELD = "f", COL_VALUE = "v";

            sm.execute("DROP TABLE IF EXISTS " + KEYSPACE + "." + TABLE);
            sm.execute("CREATE TABLE " + KEYSPACE + "." + TABLE + "(" + COL_KEY + " text,"
                    + COL_FIELD + " text," + COL_VALUE + " blob,PRIMARY KEY((" + COL_KEY + "),"
                    + COL_FIELD + "))");
            Thread.sleep(1000);

            long t1 = System.currentTimeMillis();
            try (CassandraKdWideColumnStorage kdStore = new CassandraKdWideColumnStorage()) {
                kdStore.setColumnKey(COL_KEY).setColumnField(COL_FIELD).setColumnValue(COL_VALUE)
                        .setSessionManager(sm).setDefaultKeyspace(KEYSPACE).setAsyncDelete(true)
                        .setAsyncPut(true).init();

                System.out.println(kdStore.size(TABLE));

                for (int i = 0; i < 100; i++) {
                    String field = RandomStringUtils.randomAlphabetic(8);
                    String value = RandomStringUtils.randomAscii(1 + random.nextInt(1024));
                    Map<String, Object> doc = MapUtils.createMap(field, value);
                    kdStore.put(TABLE, String.valueOf(i), doc);
                }

                System.out.println(kdStore.size(TABLE));

                for (int i = 0; i < 100; i++) {
                    Map<String, Object> doc = kdStore.get(TABLE, String.valueOf(i));
                    System.out.println(i + ": " + doc);
                    if (random.nextInt() % 3 == 0) {
                        kdStore.delete(TABLE, String.valueOf(i));
                    }
                }

                System.out.println(kdStore.size(TABLE));
            }
            long t2 = System.currentTimeMillis();
            long d = t2 - t1;
            System.out.println("Finished in " + d + " ms.");
        }
    }
}
