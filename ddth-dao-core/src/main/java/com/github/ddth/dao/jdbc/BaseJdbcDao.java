package com.github.ddth.dao.jdbc;

import java.sql.Connection;
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
    public Connection getConnection() {
        return getConnection(false);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.8.1
     */
    @Override
    public Connection getConnection(String dsName) {
        return getConnection(dsName, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(boolean startTransaction) {
        return jdbcHelper.getConnection(startTransaction);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.8.1
     */
    @Override
    public Connection getConnection(String dsName, boolean startTransaction) {
        return jdbcHelper.getConnection(dsName, startTransaction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnConnection(Connection conn) {
        jdbcHelper.returnConnection(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean startTransaction(Connection conn) {
        return jdbcHelper.startTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean commitTransaction(Connection conn) {
        return jdbcHelper.commitTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rollbackTransaction(Connection conn) {
        return jdbcHelper.rollbackTransaction(conn);
    }

    /*--------------------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Object... bindValues) {
        return jdbcHelper.execute(sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Map<String, ?> bindValues) {
        return jdbcHelper.execute(sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Object... bindValues) {
        return jdbcHelper.execute(conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Map<String, ?> bindValues) {
        return jdbcHelper.execute(conn, sql, bindValues);
    }

    /*--------------------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        return jdbcHelper.executeSelect(rowMapper, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql,
            Map<String, ?> bindValues) {
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
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) {
        return jdbcHelper.executeSelect(rowMapper, conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Object... bindValues) {
        return jdbcHelper.executeSelect(sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Map<String, ?> bindValues) {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Map<String, ?> bindValues) {
        return jdbcHelper.executeSelect(conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        return jdbcHelper.executeSelectOne(rowMapper, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues) {
        return jdbcHelper.executeSelectOne(rowMapper, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) {
        return jdbcHelper.executeSelectOne(rowMapper, conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) {
        return jdbcHelper.executeSelectOne(rowMapper, conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Object... bindValues) {
        return jdbcHelper.executeSelectOne(sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Map<String, ?> bindValues) {
        return jdbcHelper.executeSelectOne(sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql, Object... bindValues) {
        return jdbcHelper.executeSelectOne(conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql,
            Map<String, ?> bindValues) {
        return jdbcHelper.executeSelectOne(conn, sql, bindValues);
    }

}
