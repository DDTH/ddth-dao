package com.github.ddth.dao.qnd;

import com.github.ddth.dao.BaseJsonBo;

public class QndBoAttrsMap {

    public static void main(String[] args) {
        BaseJsonBo myBo = new BaseJsonBo();

        myBo.setSubAttr("name", "first", "Thanh");
        myBo.setSubAttr("name", "last", "Nguyen");
        myBo.setSubAttr("addr", "number", 123);
        myBo.setSubAttr("addr", "street", "Abc");
        myBo.setSubAttr("addr", "surburb", "X");
        System.out.println(myBo.getAttributes());

        System.out.println(myBo.getSubAttr("addr", "number"));
        myBo.removeSubAttr("name", "last");
        myBo.removeSubAttr("addr", "surburb");
        System.out.println(myBo.getAttributes());
    }
}
