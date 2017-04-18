package com.github.ddth.dao.qnd;

import com.github.ddth.dao.BaseJsonBo;

public class QndBoJson {

    static class MyBo extends BaseJsonBo {
        protected Object getSubAttr(String attrName, String dPath) {
            return super.getSubAttr(attrName, dPath);
        }

        public <T> T getSubAttr(String attrName, String dPath, Class<T> clazz) {
            return super.getSubAttr(attrName, dPath, clazz);
        }

        public BaseJsonBo setSubAttr(String attrName, String dPath, Object value) {
            return super.setSubAttr(attrName, dPath, value);
        }

        public BaseJsonBo removeSubAttr(String attrName, String dPath) {
            return super.removeSubAttr(attrName, dPath);
        }
    }

    public static void main(String[] args) {
        MyBo myBo = new MyBo();

        myBo.setSubAttr("name", "first", "Thanh");
        myBo.setSubAttr("name", "last", "Nguyen");
        myBo.setSubAttr("addr", "number", 123);
        myBo.setSubAttr("addr", "street", "Abc");
        myBo.setSubAttr("addr", "surburb", "X");
        System.out.println(myBo.toMap());

        System.out.println(myBo.getSubAttr("addr", "number"));
        myBo.removeSubAttr("name", "last");
        myBo.removeSubAttr("addr", "surburb");
        System.out.println(myBo.toMap());
    }
}
