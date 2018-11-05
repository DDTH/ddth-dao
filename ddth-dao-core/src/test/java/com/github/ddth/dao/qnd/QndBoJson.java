package com.github.ddth.dao.qnd;

import java.util.Date;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.ddth.dao.BaseJsonBo;

public class QndBoJson {

    public static class MyBo extends BaseJsonBo {
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

        public Map<String, JsonNode> getCacheJsonObjs() {
            return cacheJsonObjs;
        }
    }

    public static void main(String[] args) {
        MyBo org = MyBo.newInstance(1);
        org.setAttribute("name", "DDTH");
        org.setAttribute("year", 2018);
        org.setSubAttr("founder", "name", "Thanh Nguyen");
        org.setSubAttr("founder", "email", "btnguyen2k(at)gmail.com");

        org.setSubAttr("addr", "number", 123);
        org.setSubAttr("addr", "street", "Abc");
        org.setSubAttr("addr", "surburb", "X");

        org.setSubAttr("employees", "developers.[0].name", "Thanh Nguyen");
        org.setSubAttr("employees", "developers[0].email", "btnguyen2k(at)gmail.com");
        org.setSubAttr("employees", "developers.[1].name", "Thanh Ba Nguyen");
        org.setSubAttr("employees", "developers[1].email", "btnguyen2k(at)yahoo.com");
        org.setSubAttr("employees", "admin.name.first", "Thanh");
        org.setSubAttr("employees", "admin.name.last", "Nguyen");

        Map<String, Object> attrs = org.getAttributes();
        System.out.println(attrs.get("name"));
        System.out.println(attrs.get("year"));

        System.out.println(attrs.get("founder"));
        System.out.println(org.getSubAttr("founder", "name"));
        System.out.println(org.getSubAttr("founder", "email"));

        System.out.println(org.getSubAttr("employees", "developers"));
        System.out.println(org.getSubAttr("employees", "developers[0]"));
        System.out.println(org.getSubAttr("employees", "developers.[1].name"));
        System.out.println(org.getSubAttr("employees", "admin.name.first"));

        // System.out.println(company.getSubAttr("name", "first"));
        // System.out.println(company.getSubAttr("name", "first", String.class));
        // System.out.println(company.getAttribute("name"));
        // System.out.println(company.getAttributesAsJsonString());
        //
        // company.removeSubAttr("name", "last");
        // company.removeSubAttr("addr", "surburb");
        System.out.println(org.getCacheJsonObjs());
        System.out.println(org.getAttributesAsJson());
    }
}
