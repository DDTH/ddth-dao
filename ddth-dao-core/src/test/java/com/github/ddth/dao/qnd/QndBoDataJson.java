package com.github.ddth.dao.qnd;

import java.util.Date;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.ddth.dao.BaseDataJsonFieldBo;

public class QndBoDataJson {

    public static class MyBo extends BaseDataJsonFieldBo {
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
        MyBo org = MyBo.newInstance(1);
        org.setName("DDTH");
        org.setTimestamp(new Date());
        System.out.println("ID: " + org.getId());

        org.setDataAttr("year", 2018);
        System.out.println(org.getDataAttrOptional("year", Integer.class).orElse(0));
        org.setDataAttr("msg", "Hello, world!");
        System.out.println(org.getDataAttr("msg", String.class));

        System.out.println(org.getAttributesAsJsonString());

        org.setDataAttr("founder.name", "Thanh Nguyen");
        org.setDataAttr("founder.email", "btnguyen2k(at)gmail.com");

        org.setDataAttr("addr.number", 123);
        org.setDataAttr("addr.street", "Abc");
        org.setDataAttr("addr.surburb", "X");

        org.setDataAttr("employees.developers.[0].name", "Thanh Nguyen");
        org.setDataAttr("employees.developers[0].email", "btnguyen2k(at)gmail.com");
        org.setDataAttr("employees.developers.[1].name", "Thanh Ba Nguyen");
        org.setDataAttr("employees.developers[1].email", "btnguyen2k(at)yahoo.com");
        org.setDataAttr("employees.admin.name.first", "Thanh");
        org.setDataAttr("employees.admin.name.last", "Nguyen");

        JsonNode dataAttrs = org.getDataAttrs();
        System.out.println(dataAttrs.get("year"));
        System.out.println(dataAttrs.get("msg"));

        System.out.println(dataAttrs.get("founder"));
        System.out.println(org.getDataAttr("founder.name"));
        System.out.println(org.getDataAttr("founder.email"));

        System.out.println(org.getDataAttr("employees.developers"));
        System.out.println(org.getDataAttr("employees.developers[0]"));
        System.out.println(org.getDataAttr("employees.developers.[1].name"));
        System.out.println(org.getDataAttr("employees.admin.name.first"));

        org.removeDataAttr("year");
        org.removeDataAttr("addr.surburb");
        // System.out.println(org.getCacheJsonObjs());
        System.out.println(org.getAttributesAsJson());
    }
}
