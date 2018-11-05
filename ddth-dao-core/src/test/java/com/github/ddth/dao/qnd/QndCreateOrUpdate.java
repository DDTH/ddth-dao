package com.github.ddth.dao.qnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Savepoint;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;

public class QndCreateOrUpdate {

    private static Connection getConnectionMysql() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://10.100.174.14/ghn_test?useSSL=false",
                "ghn_test", "testpassword");
    }

    private static DataSource getDataSourceMysql() throws SQLException {
        Connection conn = getConnectionMysql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndMysqlCreateOrUpdateNoTx(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        try (Connection conn = jdbcHelper.getConnection(false)) {
            jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
            Savepoint savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
            try {
                jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 1, "b");
            } catch (Exception e) {
                System.out.println(e.getMessage());
                if (savepoint != null) {
                    conn.rollback(savepoint);
                }
            }
            jdbcHelper.execute(conn, "UPDATE tbl_testdup SET username=? WHERE id=?", "c", 1);
        }
    }

    private static void qndMysqlCreateOrUpdateTx(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        try (Connection conn = jdbcHelper.getConnection(true)) {
            jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 2, "x");
            Savepoint savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
            try {
                jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 2, "y");
            } catch (Exception e) {
                System.out.println(e.getMessage());
                if (savepoint != null) {
                    conn.rollback(savepoint);
                }
            }
            jdbcHelper.execute(conn, "UPDATE tbl_testdup SET username=? WHERE id=?", "z", 2);
        }
    }

    private static Connection getConnectionPgsql() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://10.100.174.30/ghn_test", "ghn_test",
                "testpassword");
    }

    private static DataSource getDataSourcePgsql() throws SQLException {
        Connection conn = getConnectionPgsql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndPgsqlCreateOrUpdateNoTx(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        try (Connection conn = jdbcHelper.getConnection(false)) {
            jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
            Savepoint savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
            try {
                jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 1, "b");
            } catch (Exception e) {
                System.out.println(e.getMessage());
                if (savepoint != null) {
                    conn.rollback(savepoint);
                }
            }
            jdbcHelper.execute(conn, "UPDATE tbl_testdup SET username=? WHERE id=?", "c", 1);
        }
    }

    private static void qndPgsqlCreateOrUpdateTx(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        try (Connection conn = jdbcHelper.getConnection(true)) {
            jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 2, "x");
            Savepoint savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
            try {
                jdbcHelper.execute(conn, "INSERT INTO tbl_testdup VALUES (?,?)", 2, "y");
            } catch (Exception e) {
                System.out.println(e.getMessage());
                if (savepoint != null) {
                    conn.rollback(savepoint);
                }
            }
            jdbcHelper.execute(conn, "UPDATE tbl_testdup SET username=? WHERE id=?", "z", 2);
        }
    }

    // private static Connection getConnectionMssql() throws SQLException {
    // return DriverManager.getConnection("jdbc:sqlserver://localhost;databaseName=tempdb", "sa",
    // "S3cr3tP2ssw0rd!");
    // }
    //
    // private static DataSource getDataSourceMssql() throws SQLException {
    // Connection conn = getConnectionMssql();
    // return new SingleConnectionDataSource(conn, true);
    // }
    //
    // private static void qndMssqlPK(IJdbcHelper jdbcHelper) throws SQLException {
    // jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
    // jdbcHelper.execute(
    // "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
    // jdbcHelper.execute("CREATE UNIQUE INDEX idx_username ON tbl_testdup(username)");
    // jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
    // jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "b");
    // }
    //
    // private static void qndMssqlUnique(IJdbcHelper jdbcHelper) throws SQLException {
    // jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
    // jdbcHelper.execute(
    // "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
    // jdbcHelper.execute("CREATE UNIQUE INDEX idx_username ON tbl_testdup(username)");
    // jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
    // jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 2, "a");
    // }

    public static void main(String[] args) throws Exception {
        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            System.out.println("-= MySQL: DdthJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            qndMysqlCreateOrUpdateNoTx(jdbcHelper);
            qndMysqlCreateOrUpdateTx(jdbcHelper);
        }

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            System.out.println("-= PgSQL: DdthJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourcePgsql()).init();
            qndPgsqlCreateOrUpdateNoTx(jdbcHelper);
            qndPgsqlCreateOrUpdateTx(jdbcHelper);
        }
    }
}
