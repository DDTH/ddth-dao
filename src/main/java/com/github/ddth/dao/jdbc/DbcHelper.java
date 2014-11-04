package com.github.ddth.dao.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.sql.DataSource;

/**
 * Database Connectivity Helper class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.2.0
 */
public class DbcHelper {
    private final static ConcurrentMap<String, DataSource> jdbcDataSources = new ConcurrentHashMap<String, DataSource>();

    public static void init() {
        jdbcDataSources.clear();
    }

    public static void destroy() {
        jdbcDataSources.clear();
    }

    /**
     * Registers a named JDBC datasource.
     * 
     * @param name
     * @param dataSource
     * @return
     */
    public static boolean registerJdbcDataSource(String name, DataSource dataSource) {
        return jdbcDataSources.putIfAbsent(name, dataSource) == null;
    }

    /**
     * Unregisters a JDBC datasource by name.
     * 
     * @param name
     * @return
     */
    public static boolean unregisterJdbcDataSource(String name) {
        return jdbcDataSources.remove(name) != null;
    }

    /**
     * Retrieves a JDBC datasource by name.
     * 
     * @param name
     * @return
     */
    public static DataSource getJdbcDataSource(String name) {
        return jdbcDataSources.get(name);
    }

    /*----------------------------------------------------------------------*/

    private static class OpenConnStats {
        // public String dsName;
        public Connection conn;
        public AtomicLong counter = new AtomicLong();
        public boolean inTransaction = false;
    }

    private static ThreadLocal<Map<String, OpenConnStats>> openConnStats = new ThreadLocal<Map<String, OpenConnStats>>() {
        @Override
        protected Map<String, OpenConnStats> initialValue() {
            return new ConcurrentHashMap<String, OpenConnStats>();
        }
    };

    private static ThreadLocal<Map<Connection, String>> openConnDsName = new ThreadLocal<Map<Connection, String>>() {
        @Override
        protected Map<Connection, String> initialValue() {
            return new ConcurrentHashMap<Connection, String>();
        }
    };

    /**
     * Obtains a JDBC connection from a named data-source (with no transaction
     * enabled).
     * 
     * <p>
     * Note: call {@link #returnConnection(Connection)} to return the connection
     * back to the pool. Do NOT use {@code Connection.clode()}.
     * </p>
     * 
     * @param dataSourceName
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String dataSourceName) throws SQLException {
        return getConnection(dataSourceName, false);
    }

    /**
     * Obtains a JDBC connection from a named data-source (start a new
     * transaction if specified).
     * 
     * <p>
     * Note: call {@link #returnConnection(Connection)} to return the connection
     * back to the pool. Do NOT use {@code Connection.clode()}.
     * </p>
     * 
     * @param dataSourceName
     * @param startTransaction
     * @return
     * @throws SQLException
     */
    public static Connection getConnection(String dataSourceName, boolean startTransaction)
            throws SQLException {
        Map<String, OpenConnStats> statsMap = openConnStats.get();
        OpenConnStats connStats = statsMap.get(dataSourceName);
        Connection conn;

        if (connStats == null) {
            // no existing connection, obtain a new one
            conn = getJdbcDataSource(dataSourceName).getConnection();
            openConnDsName.get().put(conn, dataSourceName);

            connStats = new OpenConnStats();
            connStats.conn = conn;
            // connStats.dsName = dataSourceName;
            statsMap.put(dataSourceName, connStats);

            connStats.inTransaction = startTransaction;
            if (startTransaction) {
                conn.setAutoCommit(false);
            } else {
                conn.setAutoCommit(true);
            }
        } else {
            conn = connStats.conn;
        }
        connStats.counter.incrementAndGet();
        return conn;
    }

    /**
     * Returns a JDBC connection obtained from {@link #getConnection(String)}.
     * 
     * @param conn
     * @throws SQLException
     */
    public static void returnConnection(Connection conn) throws SQLException {
        if (conn == null) {
            return;
        }
        String dsName = openConnDsName.get().get(conn);
        OpenConnStats connStats = dsName != null ? openConnStats.get().get(dsName) : null;
        if (connStats == null) {
            conn.close();
        } else {
            long value = connStats.counter.decrementAndGet();
            if (value <= 0) {
                try {
                    try {
                        if (connStats.inTransaction) {
                            conn.commit();
                        }
                    } catch (Exception e) {
                        conn.rollback();
                    } finally {
                        conn.close();
                    }
                } finally {
                    openConnStats.get().remove(dsName);
                    openConnDsName.get().remove(conn);
                }
            }
        }
    }
}
