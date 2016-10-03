package com.github.ddth.dao.qnd;

import com.github.ddth.dao.BaseBo;

public class QndBoSerialization {
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

        // {
        // String json = bo.toJson();
        // System.out.println("Json\t:" + json);
        // MyBo bo2 = new MyBo();
        // bo2.fromJson(json);
        // System.out.println("BO2\t:" + bo2);
        // System.out.println("Compare\t: " + bo.equals(bo2));
        // }

        // byte[] data1 = bo.toByteArray();
        // System.out.println(bo.name("").fromByteArray(data1));

        // String data2 = bo.toJson();
        // System.out.println(bo.name("").fromJson(data2));

        // byte[] data1 = SerializationUtils.toByteArray(bo);
        // MyBo bo1 = (MyBo) SerializationUtils.fromByteArray(data1);
        // System.out.println(bo1);
        //
        // byte[] data2 = SerializationUtils.toByteArrayKryo(bo);
        // Object bo2 = SerializationUtils.fromByteArrayKryo(data2, MyBo.class);
        // System.out.println(bo2);
    }
}
