package com.github.ddth.dao.qnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;

public class QndDateTime {

    public static void main(String[] args) throws Exception {
        String hostAndPort = System.getProperty("mysql.hostAndPort", "localhost:3306");
        String user = System.getProperty("mysql.user", "travis");
        String password = System.getProperty("mysql.pwd", "");
        String db = System.getProperty("mysql.db", "test");
        String url = "jdbc:mysql://" + hostAndPort + "/" + db
                + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        DataSource ds = new SimpleDriverDataSource(DriverManager.getDriver(url), url, user,
                password);

        try (DdthJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(ds);
            jdbcHelper.init();

            jdbcHelper.execute("DROP TABLE IF EXISTS tbl_test");
            jdbcHelper.execute("CREATE TABLE tbl_test ("
                    + "id INT, col_date DATE, col_time TIME, col_datetime DATETIME, col_timestamp TIMESTAMP, "
                    + "PRIMARY KEY (id)) Engine=InnoDB");

            try (Connection conn = jdbcHelper.getConnection()) {
                final int ID = 1;
                final Date NOW = new Date();

                PreparedStatement pstm = conn.prepareStatement(
                        "INSERT INTO tbl_test (id, col_date, col_time, col_datetime, col_timestamp) VALUES (?, ?, ?, ?, ?)");
                pstm.setInt(1, ID);
                pstm.setObject(2, NOW);
                pstm.setObject(3, NOW);
                pstm.setObject(4, NOW);
                pstm.setObject(5, NOW);
                pstm.execute();
            }

            // jdbcHelper.execute(
            // "INSERT INTO tbl_test (id, col_date, col_time, col_datetime)
            // VALUES (?, ?, ?, ?)",
            // ID, NOW, NOW, NOW);
            //
            // PreparedStatement pstm =jdbcHelper.
            //

            ResultSet rs = jdbcHelper.getConnection().createStatement()
                    .executeQuery("SELECT * FROM tbl_test");
            rs.next();
            System.out.println(rs.getObject("col_date"));
            System.out.println(rs.getObject("col_time"));
            System.out.println(rs.getObject("col_datetime"));
            System.out.println(rs.getObject("col_timestamp"));

            //
            // Map<String, Object> row = jdbcHelper.executeSelectOne("SELECT *
            // FROM tbl_test");
            // System.out.println(row);
        }
    }

}
