package com.github.ddth.dao.jdbc;

import com.github.ddth.dao.BaseDao;
import com.github.ddth.dao.jdbc.impl.ResultSetIterator;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DatabaseVendor;
import com.github.ddth.dao.utils.DbcHelper;
import com.github.ddth.dao.utils.JdbcHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLErrorCodesFactory;
import org.springframework.jdbc.support.SQLExceptionTranslator;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
     * @return
     * @since 0.8.2
     */
    protected SQLErrorCodesFactory getSQLErrorCodesFactory() {
        return sqlErrorCodesFactory;
    }

    /**
     * @param conn
     * @return
     * @since 0.8.2
     */
    protected SQLExceptionTranslator getSQLExceptionTranslator(Connection conn) {
        DataSource dataSource = DbcHelper.getDataSource(conn);
        SQLExceptionTranslator translator = cachedSQLExceptionTranslators.get(dataSource);
        if (translator == null) {
            translator = new SQLErrorCodeSQLExceptionTranslator(sqlErrorCodesFactory.getErrorCodes(dataSource));
            SQLExceptionTranslator existing = cachedSQLExceptionTranslators.putIfAbsent(dataSource, translator);
            if (existing != null) {
                translator = existing;
            }
        }
        return translator;
    }

    /**
     * @param conn
     * @param e
     * @return
     * @since 0.8.2
     */
    protected DaoException translateSQLException(Connection conn, SQLException e) {
        return translateSQLException(conn, null, null, e);
    }

    /**
     * @param conn
     * @param task
     * @param sql
     * @param e
     * @return
     * @since 0.8.2
     */
    protected DaoException translateSQLException(Connection conn, String task, String sql, SQLException e) {
        return DaoException.translate(getSQLExceptionTranslator(conn).translate(task, sql, e));
    }

    /**
     * @param dae
     * @return
     * @since 0.8.2
     */
    protected DaoException translateSQLException(DataAccessException dae) {
        return DaoException.translate(dae);
    }

    /**
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
     * @param dsName
     * @return
     * @since 0.8.1
     */
    public DataSource getDataSource(String dsName) {
        return dataSources.get(dsName);
    }

    /**
     * @return
     * @since 0.8.1
     */
    public Map<String, DataSource> getDataSources() {
        return Collections.unmodifiableMap(dataSources);
    }

    /**
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
     *
     * @since 0.8.1
     */
    @Override
    public Connection getConnection(String dsName, boolean startTransaction) {
        try {
            Connection conn = DbcHelper.getConnection(id + "-" + dsName, startTransaction);
            if (conn != null) {
                return (Connection) Proxy
                        .newProxyInstance(getClass().getClassLoader(), new Class<?>[] { Connection.class },
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
    private int _executeAndCloseNonSelect(Connection conn, Supplier<Integer> f) {
        try {
            return f.get();
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
    public int execute(String sql, Object... bindValues) {
        Connection conn = getConnection();
        return _executeAndCloseNonSelect(conn, () -> execute(conn, sql, bindValues));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Map<String, ?> bindValues) {
        Connection conn = getConnection();
        return _executeAndCloseNonSelect(conn, () -> execute(conn, sql, bindValues));
    }

    /*----------------------------------------------------------------------*/
    private <T> List<T> _executeAndCloseSelect(Connection conn, Supplier<List<T>> f) {
        try {
            return f.get();
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
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        Connection conn = getConnection();
        return _executeAndCloseSelect(conn, () -> executeSelect(rowMapper, conn, sql, bindValues));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues) {
        Connection conn = getConnection();
        return _executeAndCloseSelect(conn, () -> executeSelect(rowMapper, conn, sql, bindValues));
    }

    /*----------------------------------------------------------------------*/
    private <T> T _executeAndCloseSelectOne(Connection conn, Supplier<T> f) {
        try {
            return f.get();
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
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        Connection conn = getConnection();
        return _executeAndCloseSelectOne(conn, () -> executeSelectOne(rowMapper, conn, sql, bindValues));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues) {
        Connection conn = getConnection();
        return _executeAndCloseSelectOne(conn, () -> executeSelectOne(rowMapper, conn, sql, bindValues));
    }

    /*----------------------------------------------------------------------*/

    /**
     * Calculate fetch size used for streaming.
     *
     * <p>Difference db-drivers/vendors accept different fetch-size values for result-set streaming.</p>
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

    private <T> Stream<T> _executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn, boolean autoCloseConnection,
            String sql, Supplier<PreparedStatement> pstmCreator) {
        long timestampStart = System.currentTimeMillis();
        try {
            DatabaseVendor dbVendor = DbcHelper.detectDbVendor(conn);
            if (dbVendor == DatabaseVendor.POSTGRESQL && conn.getAutoCommit()) {
                /*
                 * PostgreSQL: autoCommit must be off!
                 */
                conn.setAutoCommit(false);
            }
            PreparedStatement pstm = pstmCreator.get();
            ResultSetIterator<T> rsi = autoCloseConnection ?
                    new ResultSetIterator<>(conn, rowMapper, pstm) :
                    new ResultSetIterator<>(rowMapper, pstm);
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(rsi, Spliterator.IMMUTABLE), false)
                    .onClose(rsi::close);
        } catch (SQLException | DaoException e) {
            try {
                if (autoCloseConnection) {
                    conn.close();
                }
            } catch (SQLException e1) {
                LOGGER.warn(e1.getMessage(), e1);
            }
            if (e instanceof SQLException) {
                throw translateSQLException(conn, "executeSelectAsStream", sql, (SQLException) e);
            } else {
                throw (DaoException) e;
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn, boolean autoCloseConnection,
            int fetchSize, String sql, Object... bindValues) {
        return _executeSelectAsStream(rowMapper, conn, autoCloseConnection, sql, () -> {
            try {
                /*
                 * Do not close the statement, ResultSetIterator will do it!
                 */
                PreparedStatement pstm = conn
                        .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                JdbcHelper.bindParams(pstm, bindValues);
                pstm.setFetchSize(calcFetchSizeForStream(fetchSize, conn));
                return pstm;
            } catch (SQLException e) {
                throw translateSQLException(conn, "executeSelectAsStream", sql, e);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn, boolean autoCloseConnection,
            int fetchSize, String sql, Map<String, ?> bindValues) {
        return _executeSelectAsStream(rowMapper, conn, autoCloseConnection, sql, () -> {
            try {
                /*
                 * Do not close the statement, ResultSetIterator will do it!
                 */
                PreparedStatement pstm = JdbcHelper
                        .prepareAndBindNamedParamsStatement(conn, sql, ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY, bindValues);
                pstm.setFetchSize(calcFetchSizeForStream(fetchSize, conn));
                return pstm;
            } catch (SQLException e) {
                throw translateSQLException(conn, "executeSelectAsStream", sql, e);
            }
        });
    }
}
