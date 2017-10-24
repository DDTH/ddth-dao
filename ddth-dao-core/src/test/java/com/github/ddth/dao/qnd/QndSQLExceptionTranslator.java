package com.github.ddth.dao.qnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLErrorCodesFactory;
import org.springframework.jdbc.support.SQLExceptionTranslator;

import com.github.ddth.dao.jdbc.DbcHelper;

public class QndSQLExceptionTranslator {

    private static Connection getConnectionMysql() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost/test?useSSL=false", "travis",
                "");
    }

    private static DataSource getDataSourceMysql() throws SQLException {
        Connection conn = getConnectionMysql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndMysqlPK(Connection conn) throws SQLException {
        conn.createStatement().execute("DROP TABLE IF EXISTS tbl_testdup");
        conn.createStatement().execute(
                "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32), unique index (username))");

        PreparedStatement pstm = conn.prepareStatement("INSERT INTO tbl_testdup VALUES (?,?)");
        DbcHelper.bindParams(pstm, 1, "a");
        pstm.execute();

        DbcHelper.bindParams(pstm, 1, "b");
        pstm.execute();
    }

    private static Connection getConnectionPgsql() throws SQLException {
        return DriverManager.getConnection("jdbc:postgresql://localhost/postgres", "postgres",
                "secretpassword");
    }

    private static DataSource getDataSourcePgsql() throws SQLException {
        Connection conn = getConnectionPgsql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndPgsqlPK(Connection conn) throws SQLException {
        // conn.createStatement().executeQuery("DROP TABLE IF EXISTS tbl_testdup");
        // jdbcHelper.execute(
        // "CREATE TABLE tbl_testdup (id int, primary key(id), username varchar(32))");
        // jdbcHelper.execute("CREATE UNIQUE INDEX idx_username ON tbl_testdup(username)");
        //
        // jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "a");
        // jdbcHelper.execute("INSERT INTO tbl_testdup VALUES (?,?)", 1, "b");
    }

    public static void main(String[] args) throws Exception {
        DataSource ds = getDataSourceMysql();
        SQLErrorCodesFactory sqlErrorCodesFactory = SQLErrorCodesFactory.getInstance();
        SQLExceptionTranslator sqlExTrans = new SQLErrorCodeSQLExceptionTranslator(
                sqlErrorCodesFactory.getErrorCodes(ds));
        try {
            qndMysqlPK(ds.getConnection());
        } catch (SQLException e) {
            System.out.println(sqlExTrans.translate(null, null, e));
            e.printStackTrace();
        }
    }
}
