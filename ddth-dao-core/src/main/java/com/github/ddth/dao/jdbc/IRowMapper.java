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
     * Transform a DB row from {@link ResultSet} to a business object.
     *
     * <p>
     * Note: Implementation should NOT call {@link ResultSet#next()}!
     * </p>
     *
     * @param rs
     * @param rowNum
     * @return the result object for the current row
     * @throws SQLException
     */
    T mapRow(ResultSet rs, int rowNum) throws SQLException;
}
