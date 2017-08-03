package com.github.ddth.dao.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

/**
 * Abstract implementation of {@link IJdbcHelper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public abstract class AbstractJdbcHelper implements IJdbcHelper {

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
    public Connection getConnection() throws SQLException {
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
    public Connection getConnection(boolean startTransaction) throws SQLException {
        Connection conn = DbcHelper.getConnection(id, startTransaction);
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { Connection.class }, new MyConnectionInvocationHandler(conn));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnConnection(Connection conn) throws SQLException {
        if (conn instanceof MyConnectionInvocationHandler) {
            DbcHelper.returnConnection(((MyConnectionInvocationHandler) conn).target);
        } else {
            DbcHelper.returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean startTransaction(Connection conn) throws SQLException {
        return DbcHelper.startTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean commitTransaction(Connection conn) throws SQLException {
        return DbcHelper.commitTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rollbackTransaction(Connection conn) throws SQLException {
        return DbcHelper.rollbackTransaction(conn);
    }

}
