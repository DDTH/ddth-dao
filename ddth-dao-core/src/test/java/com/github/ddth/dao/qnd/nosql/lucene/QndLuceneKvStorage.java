package com.github.ddth.dao.qnd.nosql.lucene;

import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.lucene.store.Directory;

import com.github.ddth.dao.nosql.lucene.LuceneKvStorage;

public class QndLuceneKvStorage extends BaseNosqlLuceneQnd {
    public static void main(String[] args) throws Exception {
        Random random = new Random(System.currentTimeMillis());
        final String SPACE = "test";

        try (Directory dir = directory(true)) {
            long t1 = System.currentTimeMillis();
            try (LuceneKvStorage kvStore = new LuceneKvStorage()) {
                kvStore.setDirectory(dir).setAsyncWrite(true).setAutoCommitPeriodMs(100).init();

                System.out.println(kvStore.size(SPACE));

                for (int i = 0; i < 100; i++) {
                    String value = RandomStringUtils.randomAscii(1 + random.nextInt(1024));
                    kvStore.put(SPACE, String.valueOf(i), value.getBytes(StandardCharsets.UTF_8));
                }

                System.out.println(kvStore.size(SPACE));

                for (int i = 0; i < 100; i++) {
                    byte[] value = kvStore.get(SPACE, String.valueOf(i));
                    System.out.println(i + ": " + new String(value, StandardCharsets.UTF_8));
                    if (random.nextInt() % 3 == 0) {
                        kvStore.delete(SPACE, String.valueOf(i));
                    }
                }

                System.out.println(kvStore.size(SPACE));
            }
            long t2 = System.currentTimeMillis();
            long d = t2 - t1;
            System.out.println("Finished in " + d + " ms.");
        }
    }
}
