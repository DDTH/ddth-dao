package com.github.ddth.dao.jdbc;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
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

    /**
     * Starts a transaction. Has no effect if already in a transaction.
     * 
     * @param conn
     * @throws SQLException
     * @since 0.4.0
     */
    public static boolean startTransaction(Connection conn) throws SQLException {
        if (conn == null) {
            return false;
        }
        String dsName = openConnDsName.get().get(conn);
        OpenConnStats connStats = dsName != null ? openConnStats.get().get(dsName) : null;
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
        if (conn == null) {
            return false;
        }
        String dsName = openConnDsName.get().get(conn);
        OpenConnStats connStats = dsName != null ? openConnStats.get().get(dsName) : null;
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
        if (conn == null) {
            return false;
        }
        String dsName = openConnDsName.get().get(conn);
        OpenConnStats connStats = dsName != null ? openConnStats.get().get(dsName) : null;
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

    /*----------------------------------------------------------------------*/
    /**
     * Bind parameters to a {@link PreparedStatement}.
     * 
     * @param pstm
     * @param bindValues
     * @since 0.8.0
     * @throws SQLException
     */
    public static void bindParams(PreparedStatement pstm, Object... bindValues)
            throws SQLException {
        for (int i = 0; i < bindValues.length; i++) {
            Object value = bindValues[i];
            if (value instanceof Boolean) {
                pstm.setBoolean(i + 1, (Boolean) value);
            } else if (value instanceof Character || value instanceof String) {
                pstm.setString(i + 1, value.toString());
            } else if (value instanceof Byte) {
                pstm.setByte(i + 1, (Byte) value);
            } else if (value instanceof Short) {
                pstm.setShort(i + 1, (Short) value);
            } else if (value instanceof Integer) {
                pstm.setInt(i + 1, (Integer) value);
            } else if (value instanceof Long) {
                pstm.setLong(i + 1, (Long) value);
            } else if (value instanceof BigInteger) {
                pstm.setLong(i + 1, ((BigInteger) value).longValue());
            } else if (value instanceof Float) {
                pstm.setFloat(i + 1, (Float) value);
            } else if (value instanceof Double) {
                pstm.setDouble(i + 1, (Double) value);
            } else if (value instanceof BigDecimal) {
                pstm.setBigDecimal(i + 1, (BigDecimal) value);
            } else if (value instanceof java.sql.Date) {
                pstm.setDate(i + 1, (java.sql.Date) value);
            } else if (value instanceof java.sql.Time) {
                pstm.setTime(i + 1, (java.sql.Time) value);
            } else if (value instanceof java.sql.Timestamp) {
                pstm.setTimestamp(i + 1, (java.sql.Timestamp) value);
            } else if (value instanceof Date) {
                pstm.setTimestamp(i + 1, new Timestamp(((Date) value).getTime()));
            } else if (value instanceof Blob) {
                pstm.setBlob(i + 1, (Blob) value);
            } else if (value instanceof Clob) {
                pstm.setClob(i + 1, (Clob) value);
            } else if (value instanceof byte[]) {
                pstm.setBytes(i + 1, (byte[]) value);
            } else {
                pstm.setObject(i + 1, value);
            }
        }
    }
}
