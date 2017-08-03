package com.github.ddth.dao.test.bo;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.dao.BaseBo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BoMultiThreadTest extends TestCase {

    public BoMultiThreadTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(BoMultiThreadTest.class);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @org.junit.Test
    public void testMultiThreads() throws InterruptedException {
        final BaseBo myBo = new BaseBo();

        final int NUM_ATTRS = 4;
        for (int i = 0; i < NUM_ATTRS; i++) {
            myBo.setAttribute(String.valueOf(i), 0);
        }

        final int NUM_READ_THREADS = 4;
        final int NUM_WRITE_THREADS = 2;
        final int NUM_READS = 100000;
        final int NUM_WRITES = 10000;

        Thread[] readThreads = new Thread[NUM_READ_THREADS];
        for (int i = 0; i < NUM_READ_THREADS; i++) {
            readThreads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_READS; i++) {
                        String key = String.valueOf(i % NUM_ATTRS);
                        myBo.getAttribute(key);
                    }
                }
            };
            readThreads[i].start();
        }

        Thread[] writeThreads = new Thread[NUM_WRITE_THREADS];
        for (int i = 0; i < NUM_WRITE_THREADS; i++) {
            writeThreads[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < NUM_WRITES; i++) {
                        String key = String.valueOf(i % NUM_ATTRS);
                        myBo.setAttribute(key, i);
                    }
                }
            };
            writeThreads[i].start();
        }

        for (Thread t : readThreads) {
            t.join();
        }

        for (Thread t : writeThreads) {
            t.join();
        }

        assertTrue(true);
    }
}
