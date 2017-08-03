package com.github.ddth.dao.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.ResultSet;

/**
 * An interface that provides APIs to interact with the underlying JDBC.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface IJdbcHelper {

    /**
     * Obtain a {@link Connection} instance, without transaction (
     * {@code autoCommit=false}).
     * 
     * <p>
     * Note: call {@link #returnConnection(Connection)} to return the connection
     * back to the pool. Do NOT use {@code Connection.clode()}.
     * </p>
     * 
     * @return
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException;

    /**
     * Obtain a {@link Connection} instance, starts a transaction if specified.
     * 
     * <p>
     * Note: call {@link #returnConnection(Connection)} to return the connection
     * back to the pool. Do NOT use {@code Connection.clode()}.
     * </p>
     * 
     * @param startTransaction
     * @return
     * @throws SQLException
     */
    public Connection getConnection(boolean startTransaction) throws SQLException;

    /**
     * Return a previously obtained {@link Connection} via
     * {@link #getConnection()} or {@link #getConnection(boolean)}.
     * 
     * @param conn
     * @throws SQLException
     */
    public void returnConnection(Connection conn) throws SQLException;

    /**
     * Start a transaction. Has no effect if already in a transaction.
     * 
     * @param conn
     * @return
     * @throws SQLException
     */
    public boolean startTransaction(Connection conn) throws SQLException;

    /**
     * Commit a transaction. Has no effect if not in a transaction.
     * 
     * <p>
     * Note: {@code autoCommit} is set to {@code true} after calling this
     * method.
     * </p>
     * 
     * @param conn
     * @return
     * @throws SQLException
     */
    public boolean commitTransaction(Connection conn) throws SQLException;

    /**
     * Rollback a transaction. Has no effect if not in a transaction.
     * 
     * <p>
     * Note: {@code autoCommit} is set to {@code true} after calling this
     * method.
     * </p>
     * 
     * @param conn
     * @return
     * @throws SQLException
     */
    public boolean rollbackTransaction(Connection conn) throws SQLException;

    /*----------------------------------------------------------------------*/

    /**
     * Execute a non-SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return number of affected rows
     * @return SQLException
     */
    public int execute(String sql, Object... bindValues) throws SQLException;

    /**
     * Execute a non-SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return number of affected rows
     * @return SQLException
     * @since 0.8.0
     */
    public int execute(String sql, Map<String, ?> bindValues) throws SQLException;

    /**
     * Execute a non-SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return number of affected rows
     * @throws SQLException
     */
    public int execute(Connection conn, String sql, Object... bindValues) throws SQLException;

    /**
     * Execute a non-SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @throws SQLException
     */
    public int execute(Connection conn, String sql, Map<String, ?> bindValues) throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @throws SQLException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @throws SQLException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> executeSelect(String sql, Object... bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            name-based bind value
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public List<Map<String, Object>> executeSelect(String sql, Map<String, ?> bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Object... bindValues) throws SQLException;

    /**
     * Execute a SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException;

    /*----------------------------------------------------------------------*/
    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Object... bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) throws SQLException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public Map<String, Object> executeSelectOne(String sql, Object... bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param sql
     * @param bindValues
     *            name-based bind value
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public Map<String, Object> executeSelectOne(String sql, Map<String, ?> bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public Map<String, Object> executeSelectOne(Connection conn, String sql, Object... bindValues)
            throws SQLException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public Map<String, Object> executeSelectOne(Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException;
}
