package com.github.ddth.dao.qnd;

import java.util.Date;

import com.github.ddth.dao.BaseBo;

public class QndBo {
    public static class MyBo extends BaseBo {
        /**
         * Helper method to create new {@link MyBo} objects.
         * 
         * @param id
         * @return
         */
        public static MyBo newInstance(long id) {
            MyBo bo = new MyBo();
            bo.setId(id);
            return bo;
        }

        private final static String ATTR_ID = "id";
        private final static String ATTR_NAME = "name";
        private final static String ATTR_TIMESTAMP = "timestamp";

        public long getId() {
            Long id = getAttribute(ATTR_ID, Long.class);
            return id != null ? id.longValue() : 0;
        }

        public void setId(long id) {
            setAttribute(ATTR_ID, id);
        }

        public String getName() {
            return getAttribute(ATTR_NAME, String.class);
        }

        public void setName(String name) {
            setAttribute(ATTR_NAME, name != null ? name.trim() : null);
        }

        public Date getTimestamp() {
            return getAttribute(ATTR_TIMESTAMP, Date.class);
        }

        public void setTimestamp(Date timestamp) {
            setAttribute(ATTR_TIMESTAMP, timestamp != null ? timestamp : new Date());
        }
    }

    public static void main(String[] args) {
        MyBo bo = MyBo.newInstance(1);
        bo.setName("btnguyen2k");
        bo.setTimestamp(new Date());
        System.out.println("ID: " + bo.getId());

        bo.setAttribute("age", 123);
        System.out.println(bo.getAttributeOptional("age", Integer.class).orElse(0));
        bo.setAttribute("msg", "Hello, world!");
        System.out.println(bo.getAttribute("msg", String.class));

        boolean isChecked = bo.getAttributeOptional("checked", Boolean.class).orElse(false);
        System.out.println(isChecked);

        int age = bo.getAttributeOptional("age", Integer.class).orElse(0);
        System.out.println(age);

        System.out.println(bo.getAttributesAsJsonString());
    }
}
