package com.github.ddth.dao.test.bo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.github.ddth.dao.BaseBo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BoChecksumTest extends TestCase {

    public BoChecksumTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(BoChecksumTest.class);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @org.junit.Test
    public void testChecksumEmpty() {
        BaseBo bo = new BaseBo();
        Assert.assertEquals(0, bo.calcChecksum());
    }

    @org.junit.Test
    public void testChecksum() {
        Random random = new Random(System.currentTimeMillis());
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(random.nextInt(1981));
        }

        Collections.shuffle(data);
        BaseBo bo1 = new BaseBo();
        data.forEach(v -> bo1.setAttribute("" + v, v));

        Collections.shuffle(data);
        BaseBo bo2 = new BaseBo();
        data.forEach(v -> bo2.setAttribute("" + v, v));

        // System.out.println(bo1.getAttributes());
        // System.out.println(bo2.getAttributes());
        Assert.assertEquals(bo1.calcChecksum(), bo2.calcChecksum());
    }

}
