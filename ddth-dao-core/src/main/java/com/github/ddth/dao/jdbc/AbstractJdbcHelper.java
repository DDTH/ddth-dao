package com.github.ddth.dao.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLErrorCodesFactory;
import org.springframework.jdbc.support.SQLExceptionTranslator;

import com.github.ddth.dao.BaseDao;
import com.github.ddth.dao.jdbc.impl.ResultSetIterator;
import com.github.ddth.dao.jdbc.impl.UniversalRowMapper;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DatabaseVendor;
import com.github.ddth.dao.utils.DbcHelper;
import com.github.ddth.dao.utils.JdbcHelper;

/**
 * Abstract implementation of {@link IJdbcHelper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public abstract class AbstractJdbcHelper implements IJdbcHelper, AutoCloseable {

    private Logger LOGGER = LoggerFactory.getLogger(AbstractJdbcHelper.class);

    private String id = UUID.randomUUID().toString();
    private Map<String, DataSource> dataSources = new HashMap<>();
    public final static String DEFAULT_DATASOURCE = "DEFAULT";
    private SQLErrorCodesFactory sqlErrorCodesFactory = SQLErrorCodesFactory.getInstance();
    private ConcurrentMap<DataSource, SQLExceptionTranslator> cachedSQLExceptionTranslators = new ConcurrentHashMap<>();
    private int defaultFetchSize = 10;

    /**
     * Set default fetch size for SELECT statements.
     * 
     * @param fetchSize
     * @return
     * @since 0.8.2
     */
    public AbstractJdbcHelper setDefaultFetchSize(int fetchSize) {
        this.defaultFetchSize = fetchSize;
        return this;
    }

    /**
     * Get default fetch size for SELECT statements.
     * 
     * @return
     * @since 0.8.2
     */
    public int getDefaultFetchSize() {
        return defaultFetchSize;
    }

    /**
     * 
     * @return
     * @since 0.8.2
     */
    protected SQLErrorCodesFactory getSQLErrorCodesFactory() {
        return sqlErrorCodesFactory;
    }

    /**
     * 
     * @param conn
     * @return
     * @since 0.8.2
     */
    protected SQLExceptionTranslator getSQLExceptionTranslator(Connection conn) {
        DataSource dataSource = DbcHelper.getDataSource(conn);
        SQLExceptionTranslator translator = cachedSQLExceptionTranslators.get(dataSource);
        if (translator == null) {
            translator = new SQLErrorCodeSQLExceptionTranslator(
                    sqlErrorCodesFactory.getErrorCodes(dataSource));
            SQLExceptionTranslator existing = cachedSQLExceptionTranslators.putIfAbsent(dataSource,
                    translator);
            if (existing != null) {
                translator = existing;
            }
        }
        return translator;
    }

    /**
     * 
     * @param conn
     * @param e
     * @return
     * @since 0.8.2
     */
    protected DaoException translateSQLException(Connection conn, SQLException e) {
        return translateSQLException(conn, null, null, e);
    }

    /**
     * 
     * @param conn
     * @param task
     * @param sql
     * @param e
     * @return
     * @since 0.8.2
     */
    protected DaoException translateSQLException(Connection conn, String task, String sql,
            SQLException e) {
        return DaoException.translate(getSQLExceptionTranslator(conn).translate(task, sql, e));
    }

    /**
     * 
     * @param dae
     * @return
     * @since 0.8.2
     */
    protected DaoException translateSQLException(DataAccessException dae) {
        return DaoException.translate(dae);
    }

    /**
     * 
     * @param dsName
     * @param ds
     * @return
     * @since 0.8.1
     */
    public AbstractJdbcHelper setDataSource(String dsName, DataSource ds) {
        dataSources.put(dsName, ds);
        return this;
    }

    /**
     * 
     * @param dsName
     * @return
     * @since 0.8.1
     */
    public DataSource getDataSource(String dsName) {
        return dataSources.get(dsName);
    }

    /**
     * 
     * @return
     * @since 0.8.1
     */
    public Map<String, DataSource> getDataSources() {
        return Collections.unmodifiableMap(dataSources);
    }

    /**
     * 
     * @param dataSources
     * @return
     * @since 0.8.1
     */
    public AbstractJdbcHelper setDataSources(Map<String, DataSource> dataSources) {
        this.dataSources.clear();
        if (dataSources != null) {
            this.dataSources.putAll(dataSources);
        }
        return this;
    }

    public AbstractJdbcHelper setDataSource(DataSource ds) {
        return setDataSource(DEFAULT_DATASOURCE, ds);
    }

    public DataSource getDataSource() {
        return getDataSource(DEFAULT_DATASOURCE);
    }

    /**
     * Initializing method.
     * 
     * @return
     */
    public AbstractJdbcHelper init() {
        dataSources.forEach((key, ds) -> DbcHelper.registerJdbcDataSource(id + "-" + key, ds));
        return this;
    }

    /**
     * Destroying method.
     */
    public void destroy() {
        dataSources.forEach((key, ds) -> DbcHelper.unregisterJdbcDataSource(id + "-" + key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
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
     * Proxy the obtained {@link Connection} instance to override the
     * {@code close()} method.
     * 
     * @author Thanh Nguyen
     * @since 0.8.0
     */
    private class MyConnectionInvocationHandler implements InvocationHandler {

        private final Connection target;

        public MyConnectionInvocationHandler(Connection target) {
            this.target = target;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getName().equals("close")) {
                AbstractJdbcHelper.this.returnConnection(target);
                return null;
            } else {
                return method.invoke(target, args);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(boolean startTransaction) {
        return getConnection(DEFAULT_DATASOURCE, startTransaction);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.8.1
     */
    @Override
    public Connection getConnection(String dsName, boolean startTransaction) {
        try {
            Connection conn = DbcHelper.getConnection(id + "-" + dsName, startTransaction);
            if (conn != null) {
                return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[] { Connection.class },
                        new MyConnectionInvocationHandler(conn));
            }
            return null;
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnConnection(Connection conn) {
        try {
            if (conn instanceof MyConnectionInvocationHandler) {
                DbcHelper.returnConnection(((MyConnectionInvocationHandler) conn).target);
            } else {
                DbcHelper.returnConnection(conn);
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean startTransaction(Connection conn) {
        try {
            return DbcHelper.startTransaction(conn);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean commitTransaction(Connection conn) {
        try {
            return DbcHelper.commitTransaction(conn);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rollbackTransaction(Connection conn) {
        try {
            return DbcHelper.rollbackTransaction(conn);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Object... bindValues) {
        Connection conn = getConnection();
        try {
            return execute(conn, sql, bindValues);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                throw translateSQLException(conn, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Map<String, ?> bindValues) {
        Connection conn = getConnection();
        try {
            return execute(conn, sql, bindValues);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                throw translateSQLException(conn, e);
            }
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        Connection conn = getConnection();
        try {
            return executeSelect(rowMapper, conn, sql, bindValues);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                throw translateSQLException(conn, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql,
            Map<String, ?> bindValues) {
        Connection conn = getConnection();
        try {
            return executeSelect(rowMapper, conn, sql, bindValues);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                throw translateSQLException(conn, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Object... bindValues) {
        return executeSelect(UniversalRowMapper.INSTANCE, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Map<String, ?> bindValues) {
        return executeSelect(UniversalRowMapper.INSTANCE, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Object... bindValues) {
        return executeSelect(UniversalRowMapper.INSTANCE, conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelect(UniversalRowMapper.INSTANCE, conn, sql, bindValues);
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        Connection conn = getConnection();
        try {
            return executeSelectOne(rowMapper, conn, sql, bindValues);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                throw translateSQLException(conn, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues) {
        Connection conn = getConnection();
        try {
            return executeSelectOne(rowMapper, conn, sql, bindValues);
        } finally {
            try {
                conn.close();
            } catch (SQLException e) {
                throw translateSQLException(conn, e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Object... bindValues) {
        return executeSelectOne(UniversalRowMapper.INSTANCE, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Map<String, ?> bindValues) {
        return executeSelectOne(UniversalRowMapper.INSTANCE, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql, Object... bindValues) {
        return executeSelectOne(UniversalRowMapper.INSTANCE, conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelectOne(UniversalRowMapper.INSTANCE, conn, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) {
        try (Stream<T> stream = executeSelectAsStream(rowMapper, conn, 1, sql, bindValues)) {
            return stream.findFirst().orElse(null);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) {
        try (Stream<T> stream = executeSelectAsStream(rowMapper, conn, 1, sql, bindValues)) {
            return stream.findFirst().orElse(null);
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * Calculate fetch size used for streaming.
     * 
     * @param hintFetchSize
     * @param conn
     * @return
     * @throws SQLException
     */
    protected int calcFetchSizeForStream(int hintFetchSize, Connection conn) throws SQLException {
        DatabaseVendor dbVendor = DbcHelper.detectDbVendor(conn);
        switch (dbVendor) {
        case MYSQL:
            return Integer.MIN_VALUE;
        default:
            return hintFetchSize < 0 ? 1 : hintFetchSize;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, String sql,
            Object... bindValues) {
        return executeSelectAsStream(rowMapper, -1, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, int fetchSize, String sql,
            Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        /*
         * Do not close the connection, ResultSetIterator will do it!
         */
        Connection conn = getConnection();
        try {
            DatabaseVendor dbVendor = DbcHelper.detectDbVendor(conn);
            if (dbVendor == DatabaseVendor.POSTGRESQL) {
                /*
                 * PostgreSQL: autoCommit must be off!
                 */
                if (conn.getAutoCommit()) {
                    conn.setAutoCommit(false);
                }
            }
            /*
             * Do not close the statement, ResultSetIterator will do it!
             */
            PreparedStatement pstm = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            pstm.setFetchSize(calcFetchSizeForStream(fetchSize, conn));
            JdbcHelper.bindParams(pstm, bindValues);
            ResultSetIterator<T> rsi = new ResultSetIterator<>(conn, rowMapper, pstm);
            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(rsi, Spliterator.IMMUTABLE), false)
                    .onClose(rsi::close);
        } catch (SQLException e) {
            try {
                throw translateSQLException(conn, "executeSelect", sql, e);
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e1) {
                    LOGGER.warn(e1.getMessage(), e1);
                }
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) {
        return executeSelectAsStream(rowMapper, conn, false, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, String sql, Object... bindValues) {
        return executeSelectAsStream(rowMapper, conn, autoCloseConnection, -1, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            int fetchSize, String sql, Object... bindValues) {
        return executeSelectAsStream(rowMapper, conn, false, fetchSize, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            DatabaseVendor dbVendor = DbcHelper.detectDbVendor(conn);
            if (dbVendor == DatabaseVendor.POSTGRESQL) {
                /*
                 * PostgreSQL: autoCommit must be off!
                 */
                if (conn.getAutoCommit()) {
                    conn.setAutoCommit(false);
                }
            }
            /*
             * Do not close the statement, ResultSetIterator will do it!
             */
            PreparedStatement pstm = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            pstm.setFetchSize(calcFetchSizeForStream(fetchSize, conn));
            JdbcHelper.bindParams(pstm, bindValues);
            ResultSetIterator<T> rsi = autoCloseConnection
                    ? new ResultSetIterator<>(conn, rowMapper, pstm)
                    : new ResultSetIterator<>(rowMapper, pstm);
            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(rsi, Spliterator.IMMUTABLE), false)
                    .onClose(rsi::close);
        } catch (SQLException e) {
            throw translateSQLException(conn, "executeSelect", sql, e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, String sql,
            Map<String, ?> bindValues) {
        return executeSelectAsStream(rowMapper, -1, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, int fetchSize, String sql,
            Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        Connection conn = getConnection();
        try {
            DatabaseVendor dbVendor = DbcHelper.detectDbVendor(conn);
            if (dbVendor == DatabaseVendor.POSTGRESQL) {
                /*
                 * PostgreSQL: autoCommit must be off!
                 */
                if (conn.getAutoCommit()) {
                    conn.setAutoCommit(false);
                }
            }
            /*
             * Do not close the statement, ResultSetIterator will do it!
             */
            PreparedStatement pstm = JdbcHelper.prepareAndBindNamedParamsStatement(conn, sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, bindValues);
            pstm.setFetchSize(calcFetchSizeForStream(fetchSize, conn));
            ResultSetIterator<T> rsi = new ResultSetIterator<>(rowMapper, pstm);
            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(rsi, Spliterator.IMMUTABLE), false)
                    .onClose(rsi::close);
        } catch (SQLException e) {
            try {
                throw translateSQLException(conn, "executeSelect", sql, e);
            } finally {
                try {
                    if (conn != null) {
                        conn.close();
                    }
                } catch (SQLException e1) {
                    LOGGER.warn(e1.getMessage(), e1);
                }
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelectAsStream(rowMapper, conn, false, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, String sql, Map<String, ?> bindValues) {
        return executeSelectAsStream(rowMapper, conn, autoCloseConnection, -1, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            int fetchSize, String sql, Map<String, ?> bindValues) {
        return executeSelectAsStream(rowMapper, conn, false, fetchSize, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            DatabaseVendor dbVendor = DbcHelper.detectDbVendor(conn);
            if (dbVendor == DatabaseVendor.POSTGRESQL) {
                /*
                 * PostgreSQL: autoCommit must be off!
                 */
                if (conn.getAutoCommit()) {
                    conn.setAutoCommit(false);
                }
            }
            /*
             * Do not close the statement, ResultSetIterator will do it!
             */
            PreparedStatement pstm = JdbcHelper.prepareAndBindNamedParamsStatement(conn, sql,
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, bindValues);
            pstm.setFetchSize(calcFetchSizeForStream(fetchSize, conn));
            ResultSetIterator<T> rsi = autoCloseConnection
                    ? new ResultSetIterator<>(conn, rowMapper, pstm)
                    : new ResultSetIterator<>(rowMapper, pstm);
            return StreamSupport
                    .stream(Spliterators.spliteratorUnknownSize(rsi, Spliterator.IMMUTABLE), false)
                    .onClose(rsi::close);
        } catch (SQLException e) {
            throw translateSQLException(conn, "executeSelect", sql, e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(String sql, Object... bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(int fetchSize, String sql,
            Object... bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, fetchSize, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, String sql,
            Object... bindValues) {
        return executeSelectAsStream(conn, false, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, String sql, Object... bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, conn, autoCloseConnection, sql,
                bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, int fetchSize,
            String sql, Object... bindValues) {
        return executeSelectAsStream(conn, false, fetchSize, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Object... bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, conn, autoCloseConnection,
                fetchSize, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(String sql,
            Map<String, ?> bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(int fetchSize, String sql,
            Map<String, ?> bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, fetchSize, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelectAsStream(conn, false, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, String sql, Map<String, ?> bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, conn, autoCloseConnection, sql,
                bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn, int fetchSize,
            String sql, Map<String, ?> bindValues) {
        return executeSelectAsStream(conn, false, fetchSize, sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Map<String, Object>> executeSelectAsStream(Connection conn,
            boolean autoCloseConnection, int fetchSize, String sql, Map<String, ?> bindValues) {
        return executeSelectAsStream(UniversalRowMapper.INSTANCE, conn, autoCloseConnection,
                fetchSize, sql, bindValues);
    }
}
