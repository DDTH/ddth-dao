package com.github.ddth.dao.test.bo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.github.ddth.dao.BaseDataJsonFieldBo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BoDataJsonFieldChecksumTest extends TestCase {

    public BoDataJsonFieldChecksumTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(BoDataJsonFieldChecksumTest.class);
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    @org.junit.Test
    public void testChecksumEmpty() {
        BaseDataJsonFieldBo bo = new BaseDataJsonFieldBo();
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
        Collections.shuffle(data);
        BaseDataJsonFieldBo bo1 = new BaseDataJsonFieldBo();
        attrs.forEach(v -> bo1.setAttribute("" + v, v));
        data.forEach(v -> bo1.setDataAttr("" + v, v));

        Collections.shuffle(attrs);
        Collections.shuffle(data);
        BaseDataJsonFieldBo bo2 = new BaseDataJsonFieldBo();
        attrs.forEach(v -> bo2.setAttribute("" + v, v));
        data.forEach(v -> bo2.setDataAttr("" + v, v));

        Assert.assertNotEquals(bo1.getAttributes().toString(), bo2.getAttributes().toString());
        Assert.assertEquals(bo1.calcChecksum(), bo2.calcChecksum());
    }

}
