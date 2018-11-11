package com.github.ddth.dao.test.bo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.dao.BaseDataJsonFieldBo;
import com.github.ddth.dao.BaseJsonBo;

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
    public void testChecksumEmptyBaseBo() {
        BaseBo bo = new BaseBo();
        Assert.assertEquals(0, bo.calcChecksum());
    }

    @org.junit.Test
    public void testChecksumEmptyBaseJsonBo() {
        BaseJsonBo bo = new BaseJsonBo();
        Assert.assertEquals(0, bo.calcChecksum());
    }

    @org.junit.Test
    public void testChecksumEmptyBaseDataJsonFieldBo() {
        BaseDataJsonFieldBo bo = new BaseDataJsonFieldBo();
        Assert.assertEquals(0, bo.calcChecksum());
    }

    @org.junit.Test
    public void testChecksumBaseBo() {
        Random random = new Random(System.currentTimeMillis());
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(random.nextInt(1981));
        }

        Collections.shuffle(data);
        BaseBo bo1 = new BaseBo();
        data.forEach(v -> bo1.setAttribute(String.valueOf(v), v));

        Collections.shuffle(data);
        BaseBo bo2 = new BaseBo();
        data.forEach(v -> bo2.setAttribute(String.valueOf(v), v));

        // attributes are added in different orders but checksum should be the same
        Assert.assertEquals(bo1.calcChecksum(), bo2.calcChecksum());
    }

    @org.junit.Test
    public void testChecksumBaseJsonBo() {
        Random random = new Random(System.currentTimeMillis());
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            data.add(random.nextInt(1981));
        }

        Collections.shuffle(data);
        BaseJsonBo bo1 = new BaseJsonBo();
        data.forEach(v -> {
            if (v % 3 == 0) {
                bo1.setAttribute(String.valueOf(v), v);
            }
            if (v % 3 == 1) {
                bo1.setSubAttr(String.valueOf(v), "[0]", v);
            }
            if (v % 3 == 2) {
                bo1.setSubAttr(String.valueOf(v), String.valueOf(0), v - 1);
                bo1.setSubAttr(String.valueOf(v), String.valueOf(1), v - 2);
            }
        });

        Collections.shuffle(data);
        BaseJsonBo bo2 = new BaseJsonBo();
        data.forEach(v -> {
            if (v % 3 == 0) {
                bo2.setAttribute(String.valueOf(v), v);
            }
            if (v % 3 == 1) {
                bo2.setSubAttr(String.valueOf(v), "[0]", v);
            }
            if (v % 3 == 2) {
                bo2.setSubAttr(String.valueOf(v), String.valueOf(1), v - 2);
                bo2.setSubAttr(String.valueOf(v), String.valueOf(0), v - 1);
            }
        });

        // attributes are added in different orders but checksum should be the same
        Assert.assertEquals(bo1.calcChecksum(), bo2.calcChecksum());
    }

}
