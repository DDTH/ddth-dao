package com.github.ddth.dao.qnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;

public class QndPreOpenConnection {

    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost/test?useSSL=false", "travis",
                "");
    }

    private static DataSource getDataSource() throws SQLException {
        Connection conn = getConnection();
        return new SingleConnectionDataSource(conn, false);
    }

    public static void main(String[] args) throws Exception {
        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSource()).init();

            Connection conn = jdbcHelper.getConnection();
            System.out.println(conn);
            System.out.println(jdbcHelper.executeSelectOne(conn, "SELECT 1 AS value"));

            Connection conn1 = jdbcHelper.getConnection();
            System.out.println(conn1);
            System.out.println(jdbcHelper.executeSelectOne(conn1, "SELECT 2 AS value"));
            conn1.close();

            System.out.println(jdbcHelper.executeSelectOne(conn, "SELECT 3 AS value"));
            conn1.close();

            System.out.println(jdbcHelper.executeSelectOne(conn, "SELECT 4 AS value"));
            conn1.close();
        }
    }
}
