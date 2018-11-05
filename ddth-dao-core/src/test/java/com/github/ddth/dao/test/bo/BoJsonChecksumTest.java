package com.github.ddth.dao.test.bo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.github.ddth.dao.BaseJsonBo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BoJsonChecksumTest extends TestCase {

    public BoJsonChecksumTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(BoJsonChecksumTest.class);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @org.junit.Test
    public void testChecksumEmpty() {
        BaseJsonBo bo = new BaseJsonBo();
        Assert.assertEquals(0, bo.calcChecksum());
    }

    @org.junit.Test
    public void testChecksum() {
        Random random = new Random(System.currentTimeMillis());
        List<Integer> attrs = new ArrayList<>();
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            attrs.add(random.nextInt(1981));
            data.add(random.nextInt(1981));
        }

        Collections.shuffle(attrs);
        BaseJsonBo bo1 = new BaseJsonBo();
        attrs.forEach(v -> {
            if (v % 2 == 0) {
                Collections.shuffle(data);
                data.forEach(d -> bo1.setSubAttr("" + v, "" + d, d));
            } else {
                bo1.setAttribute("" + v, v);
            }
        });

        Collections.shuffle(attrs);
        BaseJsonBo bo2 = new BaseJsonBo();
        attrs.forEach(v -> {
            if (v % 2 == 0) {
                Collections.shuffle(data);
                data.forEach(d -> bo2.setSubAttr("" + v, "" + d, d));
            } else {
                bo2.setAttribute("" + v, v);
            }
        });

        // System.out.println(bo1.getAttributes());
        // System.out.println(bo2.getAttributes());
        Assert.assertNotEquals(bo1.getAttributes().toString(), bo2.getAttributes().toString());
        Assert.assertEquals(bo1.calcChecksum(), bo2.calcChecksum());
    }

}
