package com.github.ddth.dao.jdbc.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark DB table's checksum column.
 * 
 * <p>
 * Usage:
 * </p>
 * 
 * <pre>
 * &#64;ChecksumColumn("checksum")
 * public class MyUserBoRowMapper extends AbstractRowMapper<UserBo> {
 *     ....
 * }
 * </pre>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.1
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ChecksumColumn {
    String value();
}
