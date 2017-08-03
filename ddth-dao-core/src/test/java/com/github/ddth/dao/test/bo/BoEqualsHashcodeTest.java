package com.github.ddth.dao.test.bo;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;

import com.github.ddth.dao.BaseBo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BoEqualsHashcodeTest extends TestCase {

    public BoEqualsHashcodeTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(BoEqualsHashcodeTest.class);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @org.junit.Test
    public void test1() {
        BaseBo bo1 = new BaseBo();
        BaseBo bo2 = new BaseBo();

        final int NUM_ITEMS = 10;
        final String[] ITEMS = new String[NUM_ITEMS];
        for (int i = 0; i < NUM_ITEMS; i++) {
            String prefix = RandomStringUtils.randomAlphanumeric(i % 10 + 1);
            String key = prefix + String.valueOf(i);
            ITEMS[i] = key;
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            bo1.setAttribute(ITEMS[i], i);
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            int j = NUM_ITEMS - i - 1;
            bo2.setAttribute(ITEMS[j], j);
        }

        assertEquals(bo1.hashCode(), bo2.hashCode());
        assertTrue(bo1.equals(bo2));
    }

    @org.junit.Test
    public void test2() {
        BaseBo bo1 = new BaseBo();
        BaseBo bo2 = new BaseBo();

        final int NUM_ITEMS = 100;
        final String[] ITEMS = new String[NUM_ITEMS];
        for (int i = 0; i < NUM_ITEMS; i++) {
            String prefix = RandomStringUtils.randomAlphanumeric(i % 10 + 1);
            String key = prefix + String.valueOf(i);
            ITEMS[i] = key;
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            bo1.setAttribute(ITEMS[i], i);
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            int j = NUM_ITEMS - i - 1;
            bo2.setAttribute(ITEMS[j], j);
        }

        assertEquals(bo1.hashCode(), bo2.hashCode());
        assertTrue(bo1.equals(bo2));
    }

    @org.junit.Test
    public void test3() {
        BaseBo bo1 = new BaseBo();
        BaseBo bo2 = new BaseBo();

        final int NUM_ITEMS = 1000;
        final String[] ITEMS = new String[NUM_ITEMS];
        for (int i = 0; i < NUM_ITEMS; i++) {
            String prefix = RandomStringUtils.randomAlphanumeric(i % 10 + 1);
            String key = prefix + String.valueOf(i);
            ITEMS[i] = key;
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            bo1.setAttribute(ITEMS[i], i);
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            int j = NUM_ITEMS - i - 1;
            bo2.setAttribute(ITEMS[j], j);
        }

        assertEquals(bo1.hashCode(), bo2.hashCode());
        assertTrue(bo1.equals(bo2));
    }

    @org.junit.Test
    public void test4() {
        BaseBo bo1 = new BaseBo();
        BaseBo bo2 = new BaseBo();

        final int NUM_ITEMS = 10000;
        final String[] ITEMS = new String[NUM_ITEMS];
        for (int i = 0; i < NUM_ITEMS; i++) {
            String prefix = RandomStringUtils.randomAlphanumeric(i % 10 + 1);
            String key = prefix + String.valueOf(i);
            ITEMS[i] = key;
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            bo1.setAttribute(ITEMS[i], i);
        }
        for (int i = 0; i < NUM_ITEMS; i++) {
            int j = NUM_ITEMS - i - 1;
            bo2.setAttribute(ITEMS[j], j);
        }

        assertEquals(bo1.hashCode(), bo2.hashCode());
        assertTrue(bo1.equals(bo2));
    }

}
