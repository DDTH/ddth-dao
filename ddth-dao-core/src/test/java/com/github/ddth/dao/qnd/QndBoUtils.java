package com.github.ddth.dao.qnd;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.dao.utils.BoUtils;

public class QndBoUtils {
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
            String json = BoUtils.toJson(bo);
            System.out.println("Json\t:" + json);
            Object bo1 = BoUtils.fromJson(json);
            Object bo2 = BoUtils.fromJson(json, MyBo.class);
            System.out.println("BO1\t:" + bo1);
            System.out.println("BO2\t:" + bo2);
            System.out.println("Compare\t: " + bo.equals(bo1) + "\t" + bo.equals(bo2));
        }

        {
            byte[] data = BoUtils.toBytes(bo);
            System.out.println("Bytes\t:" + data);
            Object bo1 = BoUtils.fromBytes(data);
            Object bo2 = BoUtils.fromBytes(data, MyBo.class);
            System.out.println("BO1\t:" + bo1);
            System.out.println("BO2\t:" + bo2);
            System.out.println("Compare\t: " + bo.equals(bo1) + "\t" + bo.equals(bo2));
        }

    }
}
