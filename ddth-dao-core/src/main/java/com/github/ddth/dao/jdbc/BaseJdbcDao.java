package com.github.ddth.dao.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.github.ddth.dao.BaseDao;

/**
 * Base class for JDBC-based DAOs.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class BaseJdbcDao extends BaseDao implements IJdbcHelper {

    private IJdbcHelper jdbcHelper;

    public BaseJdbcDao setJdbcHelper(IJdbcHelper jdbcHelper) {
        this.jdbcHelper = jdbcHelper;
        return this;
    }

    public IJdbcHelper getJdbcHelper() {
        return jdbcHelper;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(boolean startTransaction) throws SQLException {
        return jdbcHelper.getConnection(startTransaction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnConnection(Connection conn) throws SQLException {
        jdbcHelper.returnConnection(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean startTransaction(Connection conn) throws SQLException {
        return jdbcHelper.startTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean commitTransaction(Connection conn) throws SQLException {
        return jdbcHelper.commitTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rollbackTransaction(Connection conn) throws SQLException {
        return jdbcHelper.rollbackTransaction(conn);
    }

    /*--------------------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Object... bindValues) throws SQLException {
        return jdbcHelper.execute(sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Object... bindValues) throws SQLException {
        return jdbcHelper.execute(conn, sql, bindValues);
    }

    /*--------------------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues)
            throws SQLException {
        return jdbcHelper.executeSelect(rowMapper, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) {
        return jdbcHelper.executeSelect(rowMapper, conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Object... bindValues)
            throws SQLException {
        return jdbcHelper.executeSelect(sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Object... bindValues) {
        return jdbcHelper.executeSelect(conn, sql, bindValues);
    }

}
