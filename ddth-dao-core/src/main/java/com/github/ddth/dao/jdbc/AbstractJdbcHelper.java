package com.github.ddth.dao.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import com.github.ddth.dao.utils.DaoException;

/**
 * Abstract implementation of {@link IJdbcHelper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public abstract class AbstractJdbcHelper implements IJdbcHelper, AutoCloseable {

    private String id = UUID.randomUUID().toString();
    private DataSource dataSource;

    public AbstractJdbcHelper setDataSource(DataSource ds) {
        this.dataSource = ds;
        return this;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Initializing method.
     * 
     * @return
     */
    public AbstractJdbcHelper init() {
        DbcHelper.registerJdbcDataSource(id, dataSource);
        return this;
    }

    /**
     * Destroying method.
     */
    public void destroy() {
        DbcHelper.unregisterJdbcDataSource(id);
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
            }
            return method.invoke(target, args);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(boolean startTransaction) {
        try {
            Connection conn = DbcHelper.getConnection(id, startTransaction);
            return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[] { Connection.class }, new MyConnectionInvocationHandler(conn));
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
