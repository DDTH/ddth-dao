package com.github.ddth.dao.qnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.dao.jdbc.impl.JdbcTemplateJdbcHelper;
import com.github.ddth.dao.utils.DuplicatedValueException;

public class QndDuplidatedPKorUnique {

    private static Connection getConnectionMysql() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost/test?useSSL=false", "test",
                "test");
    }

    private static DataSource getDataSourceMysql() throws SQLException {
        Connection conn = getConnectionMysql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndMysqlPK(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32), unique index (username))");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "b");
    }

    private static void qndMysqlUnique(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32), unique index (username))");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 2, "a");
    }

    private static Connection getConnectionPgsql() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost/postgres", "postgres",
                "secretpassword");
    }

    private static DataSource getDataSourcePgsql() throws SQLException {
        Connection conn = getConnectionPgsql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndPgsqlPK(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        jdbcHelper.execute("CREATE UNIQUE INDEX idx_username ON tbl_testdup(username)");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "b");
    }

    private static void qndPgsqlUnique(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        jdbcHelper.execute("CREATE UNIQUE INDEX idx_username ON tbl_testdup(username)");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 2, "a");
    }

    private static Connection getConnectionMssql() throws SQLException {
        return DriverManager.getConnection("jdbc:sqlserver://localhost;databaseName=tempdb", "sa",
                "S3cr3tP2ssw0rd!");
    }

    private static DataSource getDataSourceMssql() throws SQLException {
        Connection conn = getConnectionMssql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndMssqlPK(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        jdbcHelper.execute("CREATE UNIQUE INDEX idx_username ON tbl_testdup(username)");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "b");
    }

    private static void qndMssqlUnique(IJdbcHelper jdbcHelper) throws SQLException {
        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_testdup");
        jdbcHelper.execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        jdbcHelper.execute("CREATE UNIQUE INDEX idx_username ON tbl_testdup(username)");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
        jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 2, "a");
    }

    public static void main(String[] args) throws Exception {
        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            System.out.println("-= MySQL: DdthJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            try {
                qndMysqlPK(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
            try {
                qndMysqlUnique(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
        }

        try (AbstractJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper()) {
            System.out.println("-= MySQL: JdbcTemplateJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            try {
                qndMysqlPK(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
            try {
                qndMysqlUnique(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
        }

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            System.out.println("-= PgSQL: DdthJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourcePgsql()).init();
            try {
                qndPgsqlPK(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
            try {
                qndPgsqlUnique(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
        }

        try (AbstractJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper()) {
            System.out.println("-= PgSQL: JdbcTemplateJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourcePgsql()).init();
            try {
                qndPgsqlPK(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
            try {
                qndPgsqlUnique(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
        }

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            System.out.println("-= MSSQL: DdthJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourceMssql()).init();
            try {
                qndMssqlPK(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
            try {
                qndMssqlUnique(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
        }

        try (AbstractJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper()) {
            System.out.println("-= MSSQL: JdbcTemplateJdbcHelper =-");
            jdbcHelper.setDataSource(getDataSourceMssql()).init();
            try {
                qndMssqlPK(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
            try {
                qndMssqlUnique(jdbcHelper);
            } catch (DuplicatedValueException dke) {
                System.out.println(dke);
            }
        }
    }
}
