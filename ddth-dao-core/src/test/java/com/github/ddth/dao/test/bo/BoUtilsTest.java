package com.github.ddth.dao.test.bo;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.dao.BoUtils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BoUtilsTest extends TestCase {

    public BoUtilsTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(BoUtilsTest.class);
    }

    @SuppressWarnings("unused")
    private static class MyBo extends BaseBo {
        public String getFullname() {
            return getAttribute("fullname", String.class);
        }

        public MyBo setFullname(String name) {
            setAttribute("fullname", name);
            return this;
        }

        public int getAge() {
            Integer age = getAttribute("age", Integer.class);
            return age != null ? age.intValue() : 0;
        }

        public MyBo setAge(int age) {
            setAttribute("age", age);
            return this;
        }

        public double getRate() {
            Double rate = getAttribute("rate", Double.class);
            return rate != null ? rate.doubleValue() : 0.0;
        }

        public MyBo setRate(double rate) {
            setAttribute("rate", rate);
            return this;
        }
    }

    private MyBo myBo;

    @Before
    public void setUp() {
        myBo = new MyBo();
        myBo.setFullname("Nguyen Ba Thanh").setAge(30).setRate(1.5);
    }

    @After
    public void tearDown() {
    }

    @org.junit.Test
    public void testJson() {
        String dataJson = BoUtils.toJson(myBo);

        Object bo1 = BoUtils.fromJson(dataJson);
        assertTrue(myBo.equals(bo1));

        Object bo2 = BoUtils.fromJson(dataJson, MyBo.class);
        assertTrue(myBo.equals(bo2));
    }

    @org.junit.Test
    public void testBytes() {
        byte[] dataBytes = BoUtils.toBytes(myBo);

        Object bo1 = BoUtils.fromBytes(dataBytes);
        assertTrue(myBo.equals(bo1));

        Object bo2 = BoUtils.fromBytes(dataBytes, MyBo.class);
        assertTrue(myBo.equals(bo2));
    }

}
