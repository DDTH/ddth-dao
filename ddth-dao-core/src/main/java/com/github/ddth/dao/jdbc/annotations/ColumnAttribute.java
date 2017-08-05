package com.github.ddth.dao.jdbc.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark {@code db-table-column -> bo-attribute & type}.
 * 
 * <p>
 * Usage:
 * </p>
 * 
 * <pre>
 * &#64;ColumnAttribute(column = "id", attr = "id", attrClass = long.class)
 * &#64;ColumnAttribute(column = "username", attr = "username", attrClass = String.class)
 * &#64;ColumnAttribute(column = "yob", attr = "yob", attrClass = int.class)
 * &#64;ColumnAttribute(column = "data_datetime", attr = "dataDatetime", attrClass = Date.class)
 * &#64;ColumnAttribute(column = "data_bin", attr = "dataBytes", attrClass = byte[].class)
 * public class MyUserBoRowMapper extends AbstractRowMapper<UserBo> {
 *     public long getId() {...}
 *     public void setId(long id) {...}
 *     public String getUsername() {...}
 *     public void setUsername(String id) {...}
 *     public int getYob() {...}
 *     public void setYob(String id) {...}
 *     public Date getDataDatetime() {...}
 *     public void setDataDatetime(Data datetime) {...}
 *     public byte[] getDataBytes() {...}
 *     public void setDataBytes(byte[] dataBytes) {...}
 * }
 * </pre>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Repeatable(ColumnAttributeMappings.class)
public @interface ColumnAttribute {
    /**
     * DB table column name.
     * 
     * @return
     */
    String column();

    /**
     * BO attribute name.
     * 
     * @return
     */
    String attr();

    /**
     * BO attribute type.
     * 
     * @return
     */
    Class<?> attrClass();
}
