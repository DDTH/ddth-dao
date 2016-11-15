package com.github.ddth.dao.qnd;

import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.dao.BaseBo;

public class QndBoSerialization {
    public static class MyBo extends BaseBo {
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

    public static void main(String[] args) {
        MyBo bo = new MyBo();
        bo.setFullname("Nguyen Ba Thanh").setAge(30).setRate(1.5);
        System.out.println("BO\t:" + bo);

        {
            String json = bo.toJson();
            System.out.println("Json\t:" + json);
            MyBo bo2 = new MyBo();
            bo2.fromJson(json);
            System.out.println("BO2\t:" + bo2);
            System.out.println("Compare\t: " + bo.equals(bo2));
        }

        {
            byte[] data = bo.toByteArray();
            System.out.println("Bytes\t:" + data);
            MyBo bo2 = new MyBo();
            bo2.fromByteArray(data);
            System.out.println("BO2\t:" + bo2);
            System.out.println("Compare\t: " + bo.equals(bo2));
        }

        {
            byte[] data = SerializationUtils.toByteArray(bo);
            System.out.println("Bytes\t:" + data);
            MyBo bo2 = SerializationUtils.fromByteArray(data, MyBo.class);
            System.out.println("BO2\t:" + bo2);
            System.out.println("Compare\t: " + bo.equals(bo2));
        }

        {
            byte[] data = SerializationUtils.toByteArrayFst(bo);
            System.out.println("Bytes\t:" + data);
            MyBo bo2 = SerializationUtils.fromByteArrayFst(data, MyBo.class);
            System.out.println("BO2\t:" + bo2);
            System.out.println("Compare\t: " + bo.equals(bo2));
        }

        {
            byte[] data = SerializationUtils.toByteArrayKryo(bo);
            System.out.println("Bytes\t:" + data);
            MyBo bo2 = SerializationUtils.fromByteArrayKryo(data, MyBo.class);
            System.out.println("BO2\t:" + bo2);
            System.out.println("Compare\t: " + bo.equals(bo2));
        }
    }
}
