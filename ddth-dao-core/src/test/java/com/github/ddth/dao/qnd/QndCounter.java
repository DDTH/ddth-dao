package com.github.ddth.dao.qnd;

public class QndCounter {
    volatile protected int count = 0;

    public void add(int value) {
        while (value-- > 0) {
            this.count++;
        }
    }

    public static void main(String[] args) throws Exception {
        final QndCounter COUNTER = new QndCounter();
        final int NUM_THREADS = 4;
        final int NUM_LOOPS = 1000;
        final int VALUE = 4;

        for (int i = 0; i < NUM_THREADS; i++) {
            Thread t = new Thread() {
                public void run() {
                    for (int j = 0; j < NUM_LOOPS; j++) {
                        COUNTER.add(VALUE);
                    }
                }
            };
            t.start();
        }

        Thread.sleep(5000);

        System.out.println(COUNTER.count);
        System.out.println(NUM_THREADS * NUM_LOOPS * VALUE);
    }
}
