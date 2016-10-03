package com.github.ddth.dao.test;

import java.util.Map;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.dao.BaseBo;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class BoSerializationTest extends TestCase {

    public BoSerializationTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(BoSerializationTest.class);
    }

    static class MyBo extends BaseBo {
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
    public void testMap() {
        Map<String, Object> dataMap = myBo.toMap();

        MyBo bo = new MyBo();
        bo.fromMap(dataMap);

        assertEquals(dataMap, bo.toMap());
        assertTrue(myBo.equals(bo));
    }

    @org.junit.Test
    public void testJson() {
        String dataJson = myBo.toJson();

        MyBo bo = new MyBo();
        bo.fromJson(dataJson);

        assertEquals(dataJson.length(), bo.toJson().length());
        assertTrue(myBo.equals(bo));
    }

    @org.junit.Test
    public void testByteArray() {
        byte[] dataBytes = myBo.toByteArray();

        MyBo bo = new MyBo();
        bo.fromByteArray(dataBytes);

        assertEquals(dataBytes.length, bo.toByteArray().length);
        assertTrue(myBo.equals(bo));
    }

    @org.junit.Test
    public void testBytes() {
        byte[] dataBytes = myBo.toBytes();

        MyBo bo = new MyBo();
        bo.fromBytes(dataBytes);

        assertEquals(dataBytes.length, bo.toBytes().length);
        assertTrue(myBo.equals(bo));
    }
}
