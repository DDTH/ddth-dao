package com.github.ddth.dao.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLErrorCodesFactory;
import org.springframework.jdbc.support.SQLExceptionTranslator;

import com.github.ddth.dao.utils.DaoException;

/**
 * Abstract implementation of {@link IJdbcHelper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public abstract class AbstractJdbcHelper implements IJdbcHelper, AutoCloseable {

    private String id = UUID.randomUUID().toString();
    private Map<String, DataSource> dataSources = new HashMap<>();
    public final static String DEFAULT_DATASOURCE = "DEFAULT";
    private SQLErrorCodesFactory sqlErrorCodesFactory = SQLErrorCodesFactory.getInstance();
    private ConcurrentMap<DataSource, SQLExceptionTranslator> cachedSQLExceptionTranslators = new ConcurrentHashMap<>();

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

}
