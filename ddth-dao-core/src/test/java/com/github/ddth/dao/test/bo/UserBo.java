package com.github.ddth.dao.test.bo;

import java.util.Base64;
import java.util.Date;

import com.github.ddth.dao.BaseBo;

/**
 * Sample class for testing.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class UserBo extends BaseBo {

    public final static String ATTR_ID = "id";
    public final static String ATTR_USERNAME = "username";
    public final static String ATTR_FULLNAME = "fullname";
    public final static String ATTR_YOB = "yob";
    public final static String ATTR_DATE = "d";
    public final static String ATTR_TIME = "t";
    public final static String ATTR_DATETIME = "dt";
    public final static String ATTR_BYTES = "bytes";
    public final static String ATTR_NOTNULL = "notnull";

    public long getId() {
        return getAttributeOptional(ATTR_ID, Long.class).orElse(Long.valueOf(0));
    }

    public UserBo setId(long id) {
        setAttribute(ATTR_ID, id);
        return this;
    }

    public String getUsername() {
        return getAttribute(ATTR_USERNAME, String.class);
    }

    public UserBo setUsername(String username) {
        setAttribute(ATTR_USERNAME, username);
        return this;
    }

    public String getFullname() {
        return getAttribute(ATTR_FULLNAME, String.class);
    }

    public UserBo setFullname(String fullname) {
        setAttribute(ATTR_FULLNAME, fullname);
        return this;
    }

    public int getYob() {
        return getAttributeOptional(ATTR_YOB, Integer.class).orElse(Integer.valueOf(0));
    }

    public UserBo setYob(int yob) {
        setAttribute(ATTR_YOB, yob);
        return this;
    }

    public Date getDataDate() {
        return getAttribute(ATTR_DATE, Date.class);
    }

    public UserBo setDataDate(Date dataDate) {
        setAttribute(ATTR_DATE, dataDate);
        return this;
    }

    public Date getDataTime() {
        return getAttribute(ATTR_TIME, Date.class);
    }

    public UserBo setDataTime(Date dataTime) {
        setAttribute(ATTR_TIME, dataTime);
        return this;
    }

    public Date getDataDatetime() {
        return getAttribute(ATTR_DATETIME, Date.class);
    }

    public UserBo setDataDatetime(Date dataDatetime) {
        setAttribute(ATTR_DATETIME, dataDatetime);
        return this;
    }

    public Integer getNotNull() {
        return getAttribute(ATTR_NOTNULL, Integer.class);
    }

    public UserBo setNotNull(Integer value) {
        setAttribute(ATTR_NOTNULL, value);
        return this;
    }

    public byte[] getDataBytes() {
        Object value = getAttribute(ATTR_BYTES);
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        if (value instanceof String) {
            return Base64.getDecoder().decode(value.toString());
        }
        return null;
    }

    public UserBo setDataBytes(byte[] dataBytes) {
        setAttribute(ATTR_BYTES, dataBytes);
        return this;
    }
}
