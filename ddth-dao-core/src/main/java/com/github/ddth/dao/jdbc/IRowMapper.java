package com.github.ddth.dao.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * An interface to map {@link ResultSet} to object.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface IRowMapper<T> {
    /**
     * Implementations must implement this method to map each row of data in the
     * ResultSet.
     * 
     * <p>
     * Note: Do NOT call {@code next()} on the ResultSet!
     * </p>
     * 
     * @param rs
     * @param rowNum
     * @return the result object for the current row
     * @throws SQLException
     */
    public T mapRow(ResultSet rs, int rowNum) throws SQLException;
}
