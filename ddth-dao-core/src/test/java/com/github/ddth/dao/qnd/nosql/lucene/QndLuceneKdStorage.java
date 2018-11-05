package com.github.ddth.dao.qnd.nosql.lucene;

import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.store.Directory;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.nosql.lucene.LuceneKdStorage;

public class QndLuceneKdStorage extends BaseNosqlLuceneQnd {
    public static void main(String[] args) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        final String SPACE = "test";

        try (Directory dir = directory(true)) {
            long t1 = System.currentTimeMillis();
            try (LuceneKdStorage kdStore = new LuceneKdStorage()) {
                kdStore.setDirectory(dir).setAsyncWrite(true).setAutoCommitPeriodMs(100).init();

                System.out.println(kdStore.size(SPACE));

                for (int i = 0; i < 100; i++) {
                    String key = RandomStringUtils.randomAlphanumeric(8);
                    String value = RandomStringUtils.randomAscii(1 + random.nextInt(1024));
                    Map<String, Object> doc = MapUtils.createMap(key, value);
                    kdStore.put(SPACE, String.valueOf(i), doc);
                }

                System.out.println(kdStore.size(SPACE));

                for (int i = 0; i < 100; i++) {
                    Map<String, Object> doc = kdStore.get(SPACE, String.valueOf(i));
                    System.out.println(i + ": " + doc);
                    if (random.nextInt() % 3 == 0) {
                        kdStore.delete(SPACE, String.valueOf(i));
                    }
                }

                System.out.println(kdStore.size(SPACE));
            }
            long t2 = System.currentTimeMillis();
            long d = t2 - t1;
            System.out.println("Finished in " + d + " ms.");
        }
    }
}
