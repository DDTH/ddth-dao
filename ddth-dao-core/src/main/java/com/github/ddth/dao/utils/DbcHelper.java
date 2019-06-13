package com.github.ddth.dao.utils;

import org.apache.commons.lang3.StringUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

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
     * Retrieves a registered JDBC data source by name.
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

    private static ThreadLocal<Map<String, OpenConnStats>> openConnStats = ThreadLocal
            .withInitial(() -> new ConcurrentHashMap<>());

    private static ThreadLocal<Map<Connection, String>> openConnDsName = ThreadLocal
            .withInitial(() -> new ConcurrentHashMap<>());

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
    public static Connection getConnection(String dataSourceName, boolean startTransaction) throws SQLException {
        Map<String, OpenConnStats> statsMap = openConnStats.get();
        OpenConnStats connStats = statsMap.get(dataSourceName);
        Connection conn;

        if (connStats == null) {
            // no existing connection, obtain a new one
            DataSource ds = getJdbcDataSource(dataSourceName);
            conn = ds != null ? ds.getConnection() : null;
            if (conn == null) {
                return null;
            }
            openConnDsName.get().put(conn, dataSourceName);

            connStats = new OpenConnStats();
            connStats.conn = conn;
            // connStats.dsName = dataSourceName;
            statsMap.put(dataSourceName, connStats);

            if (!startTransaction) {
                connStats.inTransaction = false;
                conn.setAutoCommit(true);
            }
        } else {
            conn = connStats.conn;
        }
        connStats.counter.incrementAndGet();

        if (startTransaction) {
            startTransaction(conn);
        }

        return conn;
    }

    private static OpenConnStats getOpenConnStats(Connection conn) {
        String dsName = openConnDsName.get().get(conn);
        return dsName != null ? openConnStats.get().get(dsName) : null;
    }

    /**
     * Starts a transaction. Has no effect if already in a transaction.
     *
     * @param conn
     * @throws SQLException
     * @since 0.4.0
     */
    public static boolean startTransaction(Connection conn) throws SQLException {
        OpenConnStats connStats = getOpenConnStats(conn);
        if (connStats != null && !connStats.inTransaction) {
            conn.setAutoCommit(false);
            connStats.inTransaction = true;
            return true;
        }
        return false;
    }

    /**
     * Commits a transaction. Has no effect if not in a transaction.
     *
     * <p>
     * Note: {@code autoCommit} is set to {@code true} after calling this
     * method.
     * </p>
     *
     * @param conn
     * @return
     * @throws SQLException
     * @since 0.4.0
     */
    public static boolean commitTransaction(Connection conn) throws SQLException {
        OpenConnStats connStats = getOpenConnStats(conn);
        if (connStats != null && connStats.inTransaction) {
            try {
                conn.commit();
                return true;
            } finally {
                conn.setAutoCommit(true);
                connStats.inTransaction = false;
            }
        }
        return false;
    }

    /**
     * Rollbacks a transaction. Has no effect if not in a transaction.
     *
     * <p>
     * Note: {@code autoCommit} is set to {@code true} after calling this
     * method.
     * </p>
     *
     * @param conn
     * @return
     * @throws SQLException
     * @since 0.4.0
     */
    public static boolean rollbackTransaction(Connection conn) throws SQLException {
        OpenConnStats connStats = getOpenConnStats(conn);
        if (connStats != null && connStats.inTransaction) {
            try {
                conn.rollback();
                return true;
            } finally {
                conn.setAutoCommit(true);
                connStats.inTransaction = false;
            }
        }
        return false;
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
                        try {
                            conn.setAutoCommit(true);
                        } finally {
                            connStats.inTransaction = false;
                            conn.close();
                        }
                    }
                } finally {
                    openConnStats.get().remove(dsName);
                    openConnDsName.get().remove(conn);
                }
            }
        }
    }

    /**
     * Get the {@link DataSource} that hosts the specified {@link Connection}.
     *
     * @param conn
     * @return
     * @since 0.8.2
     */
    public static DataSource getDataSource(Connection conn) {
        if (conn == null) {
            return null;
        }
        String dsName = openConnDsName.get().get(conn);
        return getJdbcDataSource(dsName);
    }

    /**
     * Detect database vender info.
     *
     * @param conn
     * @return
     * @throws SQLException
     */
    public static DatabaseVendor detectDbVendor(Connection conn) throws SQLException {
        DatabaseMetaData dmd = conn.getMetaData();
        String dpn = dmd.getDatabaseProductName();
        if (StringUtils.equalsAnyIgnoreCase("MySQL", dpn)) {
            return DatabaseVendor.MYSQL;
        }
        if (StringUtils.equalsAnyIgnoreCase("PostgreSQL", dpn)) {
            return DatabaseVendor.POSTGRESQL;
        }
        if (StringUtils.equalsAnyIgnoreCase("Microsoft SQL Server", dpn)) {
            return DatabaseVendor.MSSQL;
        }
        return DatabaseVendor.UNKNOWN;
    }
}
