package com.github.ddth.dao.qnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.dao.jdbc.impl.ResultSetIterator;
import com.github.ddth.dao.jdbc.impl.UniversalRowMapper;
import com.github.ddth.dao.utils.DbcHelper;
import com.github.ddth.dao.utils.JdbcHelper;

public class QndResultSetStreaming {

    private static Connection getConnectionMysql() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/test?useSSL=false",
                "test", "test");
        conn.setAutoCommit(false);
        return conn;
    }

    private static DataSource getDataSourceMysql() throws SQLException {
        Connection conn = getConnectionMysql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static Connection getConnectionPgsql() throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:postgresql://localhost/postgres",
                "postgres", "secretpassword");
        conn.setAutoCommit(true);
        return conn;
    }

    private static DataSource getDataSourcePgsql() throws SQLException {
        Connection conn = getConnectionPgsql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static Connection getConnectionMssql() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlserver://localhost;databaseName=tempdb", "sa",
                "S3cr3tP2ssw0rd!");
    }

    private static DataSource getDataSourceMssql() throws SQLException {
        Connection conn = getConnectionMssql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static ResultSet query(Connection conn, String sql) throws SQLException {
        Statement stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        stm.setFetchSize(10);
        return stm.executeQuery(sql);
    }

    private static int execute(Connection conn, String sql) throws SQLException {
        return conn.createStatement().executeUpdate(sql);
    }

    private static void initDataMysql(Connection conn) throws SQLException {
        execute(conn,
                "CREATE TABLE IF NOT EXISTS tbl_long (id INT AUTO_INCREMENT, PRIMARY KEY(id), data TEXT)");
        try (PreparedStatement pstm = conn
                .prepareStatement("INSERT INTO tbl_long (data) VALUES (?)")) {
            String longData = StringUtils.repeat("Nguyễn Bá Thành - https://github.com/DDTH/",
                    1024);
            for (int i = 1; i <= 100000; i++) {
                pstm.setString(1, longData);
                pstm.addBatch();
                if (i % 1000 == 0) {
                    pstm.executeBatch();
                }
            }
        }
    }

    private static void initDataPgsql(Connection conn) throws SQLException {
        execute(conn,
                "CREATE TABLE IF NOT EXISTS tbl_long (id BIGSERIAL, PRIMARY KEY(id), data TEXT)");
        try (PreparedStatement pstm = conn
                .prepareStatement("INSERT INTO tbl_long (data) VALUES (?)")) {
            String longData = StringUtils.repeat("Nguyễn Bá Thành - https://github.com/DDTH/",
                    1024);
            for (int i = 1; i <= 100000; i++) {
                pstm.setString(1, longData);
                pstm.addBatch();
                if (i % 1000 == 0) {
                    pstm.executeBatch();
                }
            }
        }
    }

    private static void initDataMssql(Connection conn) throws SQLException {
        execute(conn, "CREATE TABLE tbl_long (id INT IDENTITY(1,1), PRIMARY KEY(id), data TEXT)");
        try (PreparedStatement pstm = conn
                .prepareStatement("INSERT INTO tbl_long (data) VALUES (?)")) {
            String longData = StringUtils.repeat("Nguyễn Bá Thành - https://github.com/DDTH/",
                    1024);
            for (int i = 1; i <= 100000; i++) {
                pstm.setString(1, longData);
                pstm.addBatch();
                if (i % 1000 == 0) {
                    pstm.executeBatch();
                }
            }
        }
    }

    private static void testFetchAll(ResultSet rs) throws SQLException {
        String[] colLabels = JdbcHelper.extractColumnLabels(rs);
        List<Map<String, Object>> result = new ArrayList<>();
        int numRows = 0;
        try {
            while (rs.next()) {
                numRows++;
                Map<String, Object> row = new HashMap<>();
                result.add(row);
                for (int i = 0; i < colLabels.length; i++) {
                    row.put(colLabels[i], rs.getObject(colLabels[i]));
                }
                if (numRows % 1000 == 0) {
                    System.out.println("Num rows: " + numRows);
                }
            }
        } catch (Exception e) {
            System.out.println(numRows);
            throw e;
        }
    }

    private static void testStreaming(ResultSet rs) throws SQLException {
        ResultSetIterator<Map<String, Object>> rsi = new ResultSetIterator<>(
                UniversalRowMapper.INSTANCE, rs);
        Stream<Map<String, Object>> stream = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(rsi, 0), false).onClose(rsi::close);
        long numRows = stream.count();
        System.out.println("Num rows: " + numRows);
        stream.close();
    }

    private static void testStreaming(IJdbcHelper jdbcHelper, int fetchSize) {
        Stream<Map<String, Object>> stream = jdbcHelper.executeSelectAsStream(fetchSize,
                "SELECT * FROM tbl_long");
        long numRows = stream.count();
        System.out.println("Num rows: " + numRows);
        stream.close();
    }

    private static void testStreaming(Connection conn, IJdbcHelper jdbcHelper, int fetchSize) {
        Stream<Map<String, Object>> stream = jdbcHelper.executeSelectAsStream(conn, fetchSize,
                "SELECT * FROM tbl_long");
        long numRows = stream.count();
        System.out.println("Num rows: " + numRows);
        stream.close();
    }

    private static void qndMysql(boolean init) throws SQLException {
        try (Connection conn = getConnectionMysql()) {
            System.out.println(
                    conn.getClass().getSimpleName() + ": " + DbcHelper.detectDbVendor(conn));
            if (init)
                initDataMysql(conn);
        }

        long t = System.currentTimeMillis();
        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            testStreaming(jdbcHelper, -1);
            try (Connection conn = getConnectionMysql()) {
                testStreaming(conn, jdbcHelper, -1);
            }
        }
        System.out.println("Duration: " + (System.currentTimeMillis() - t));
    }

    private static void qndPgsql(boolean init) throws SQLException {
        try (Connection conn = getConnectionPgsql()) {
            System.out.println(
                    conn.getClass().getSimpleName() + ": " + DbcHelper.detectDbVendor(conn));
            if (init)
                initDataPgsql(conn);
        }

        long t = System.currentTimeMillis();
        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSourcePgsql()).init();
            testStreaming(jdbcHelper, 32);
            try (Connection conn = getConnectionPgsql()) {
                testStreaming(conn, jdbcHelper, 32);
            }
        }
        System.out.println("Duration: " + (System.currentTimeMillis() - t));
    }

    private static void qndMssql(boolean init) throws SQLException {
        try (Connection conn = getConnectionMssql()) {
            System.out.println(
                    conn.getClass().getSimpleName() + ": " + DbcHelper.detectDbVendor(conn));
            if (init)
                initDataMssql(conn);
        }

        long t = System.currentTimeMillis();
        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSourceMssql()).init();
            testStreaming(jdbcHelper, 32);
            try (Connection conn = getConnectionMssql()) {
                testStreaming(conn, jdbcHelper, 32);
            }
        }
        System.out.println("Duration: " + (System.currentTimeMillis() - t));
    }

    public static void main(String[] args) throws SQLException {
        // qndMysql(false);
        // qndPgsql(false);
        // qndMssql(false);
    }

}
