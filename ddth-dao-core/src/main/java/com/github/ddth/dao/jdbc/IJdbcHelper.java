package com.github.ddth.dao.jdbc;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.github.ddth.dao.utils.DaoException;

/**
 * An interface that provides APIs to interact with the underlying JDBC.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface IJdbcHelper {

    /**
     * Obtain a {@link Connection} instance from the default data source, without transaction
     * ({@code autoCommit=false}).
     * 
     * @return
     * @throws DaoException
     */
    public Connection getConnection() throws DaoException;

    /**
     * Obtain a {@link Connection} instance from the specified data source, without transaction
     * ({@code autoCommit=false}).
     * 
     * @param dsName
     * @return
     * @since 0.8.1
     * @throws DaoException
     */
    public Connection getConnection(String dsName) throws DaoException;

    /**
     * Obtain a {@link Connection} instance from the default data source, starts a transaction if
     * specified.
     * 
     * @param startTransaction
     * @return
     * @throws DaoException
     */
    public Connection getConnection(boolean startTransaction) throws DaoException;

    /**
     * Obtain a {@link Connection} instance from the specified data source, starts a transaction if
     * specified.
     * 
     * @param dsName
     * @param startTransaction
     * @return
     * @since 0.8.1
     * @throws DaoException
     */
    public Connection getConnection(String dsName, boolean startTransaction) throws DaoException;

    /**
     * Return a previously obtained {@link Connection} via
     * {@link #getConnection()} or {@link #getConnection(boolean)}.
     * 
     * @param conn
     * @throws DaoException
     */
    public void returnConnection(Connection conn) throws DaoException;

    /**
     * Start a transaction. Has no effect if already in a transaction.
     * 
     * @param conn
     * @return
     * @throws DaoException
     */
    public boolean startTransaction(Connection conn) throws DaoException;

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
     * @throws DaoException
     */
    public boolean commitTransaction(Connection conn) throws DaoException;

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
     * @throws DaoException
     */
    public boolean rollbackTransaction(Connection conn) throws DaoException;

    /*----------------------------------------------------------------------*/

    /**
     * Execute a non-SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return number of affected rows
     * @return DaoException
     */
    public int execute(String sql, Object... bindValues) throws DaoException;

    /**
     * Execute a non-SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return number of affected rows
     * @return DaoException
     * @since 0.8.0
     */
    public int execute(String sql, Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a non-SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return number of affected rows
     * @throws DaoException
     */
    public int execute(Connection conn, String sql, Object... bindValues) throws DaoException;

    /**
     * Execute a non-SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @throws DaoException
     */
    public int execute(Connection conn, String sql, Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement.
     * 
     * @param rowMapper
     *            to map the {@link ResultSet} to object
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @throws DaoException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues)
            throws DaoException;

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
     * @throws DaoException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues)
            throws DaoException;

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
     * @throws DaoException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, String sql,
            Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, int fetchSize, String sql,
            Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, String sql, Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            int fetchSize, String sql, Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Object... bindValues)
            throws DaoException;

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
     * @throws DaoException
     */
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, int fetchSize, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, String sql, Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            int fetchSize, String sql, Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return result as a {@link Stream}.
     * 
     * @param rowMapper
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Map<String, ?> bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @throws DaoException
     */
    public List<Map<String, Object>> executeSelect(String sql, Object... bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement.
     * 
     * @param sql
     * @param bindValues
     *            name-based bind value
     * @return
     * @since 0.8.0
     * @throws DaoException
     */
    public List<Map<String, Object>> executeSelect(String sql, Map<String, ?> bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @throws DaoException
     */
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(String sql, Object... bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(int fetchSize, String sql,
            Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, String sql,
            Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, String sql, Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, int fetchSize,
            String sql, Object... bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Object... bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws DaoException
     */
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(String sql, Map<String, ?> bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(int fetchSize, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, String sql, Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.3
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, int fetchSize,
            String sql, Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and return the result as a {@link Stream}.
     * 
     * @param conn
     * @param autoCloseConnection
     *            if {@code true} the supplied {@link Connection} will be automatically closed when
     *            the returned {@link Stream} closes.
     * @param fetchSize
     * @param sql
     * @param bindValues
     * @return
     * @since 0.8.5.1
     * @throws DaoException
     */
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Map<String, ?> bindValues)
            throws DaoException;

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
     * @throws DaoException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Object... bindValues)
            throws DaoException;

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
     * @throws DaoException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues)
            throws DaoException;

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
     * @throws DaoException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) throws DaoException;

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
     * @throws DaoException
     */
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) throws DaoException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     * @throws DaoException
     */
    public Map<String, Object> executeSelectOne(String sql, Object... bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param sql
     * @param bindValues
     *            name-based bind value
     * @return
     * @since 0.8.0
     * @throws DaoException
     */
    public Map<String, Object> executeSelectOne(String sql, Map<String, ?> bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     * @throws DaoException
     */
    public Map<String, Object> executeSelectOne(Connection conn, String sql, Object... bindValues)
            throws DaoException;

    /**
     * Execute a SELECT statement and fetch one row.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     * @throws DaoException
     */
    public Map<String, Object> executeSelectOne(Connection conn, String sql,
            Map<String, ?> bindValues) throws DaoException;
}
