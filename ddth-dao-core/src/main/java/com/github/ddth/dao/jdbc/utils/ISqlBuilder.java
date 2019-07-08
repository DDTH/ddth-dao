package com.github.ddth.dao.jdbc.utils;

/**
 * Abstract interface to build SQL statements.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public interface ISqlBuilder {
    /**
     * Build the SQL statement with placeholders, along with bind values, ready for {@link java.sql.PreparedStatement}.
     *
     * @return
     */
    BuildSqlResult build();
}
