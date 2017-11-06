package com.github.ddth.dao.qnd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.dao.jdbc.impl.JdbcTemplateJdbcHelper;

public class QndDdthJdbcHelperINClause {

    private static Connection getConnectionMysql() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://localhost/test?useSSL=false", "test",
                "test");
    }

    private static DataSource getDataSourceMysql() throws SQLException {
        Connection conn = getConnectionMysql();
        return new SingleConnectionDataSource(conn, true);
    }

    private static void qndSelect(IJdbcHelper jdbcHelper) {
        System.out.println("-= MySQL: " + jdbcHelper.getClass().getSimpleName() + " =-");

        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_test");
        jdbcHelper.execute(
                "CREATE TABLE tbl_test(col_id INT, PRIMARY KEY (col_id), col_char VARCHAR(32), col_data VARCHAR(255))");
        for (int i = 1; i < 10; i++) {
            jdbcHelper.execute("INSERT INTO tbl_test(col_id,col_char,col_data) VALUES (?,?,?)", i,
                    "c" + i, "data-" + i);
        }

        List<Map<String, Object>> rows = jdbcHelper.executeSelect(
                "SELECT * FROM tbl_test WHERE col_id IN (:id)",
                MapUtils.createMap("id", new int[] { 1, 2, 3 }));
        System.out.println(rows);

        rows = jdbcHelper.executeSelect("SELECT * FROM tbl_test WHERE col_id IN (:id)",
                MapUtils.createMap("id", new Integer[] { 2, 4, 6 }));
        System.out.println(rows);

        rows = jdbcHelper.executeSelect("SELECT * FROM tbl_test WHERE col_char IN (:id)",
                MapUtils.createMap("id", new String[] { "c3", "c5", "c7" }));
        System.out.println(rows);
    }

    private static void qndDelete(IJdbcHelper jdbcHelper) {
        System.out.println("-= MySQL: " + jdbcHelper.getClass().getSimpleName() + " =-");

        jdbcHelper.execute("DROP TABLE IF EXISTS tbl_test");
        jdbcHelper.execute(
                "CREATE TABLE tbl_test(col_id INT, PRIMARY KEY (col_id), col_char VARCHAR(32), col_data VARCHAR(255))");
        for (int i = 1; i < 10; i++) {
            jdbcHelper.execute("INSERT INTO tbl_test(col_id,col_char,col_data) VALUES (?,?,?)", i,
                    "c" + i, "data-" + i);
        }

        List<Map<String, Object>> rows = jdbcHelper
                .executeSelect("SELECT * FROM tbl_test ORDER BY col_id");
        System.out.println(rows);

        System.out.println(jdbcHelper.execute("DELETE FROM tbl_test WHERE col_id IN (:id)",
                MapUtils.createMap("id", new Integer[] { 2, 4, 6 })));

        rows = jdbcHelper.executeSelect("SELECT * FROM tbl_test ORDER BY col_id");
        System.out.println(rows);
    }

    public static void main(String[] args) throws Exception {
        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            qndSelect(jdbcHelper);
        }

        try (AbstractJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            qndSelect(jdbcHelper);
        }

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            qndDelete(jdbcHelper);
        }

        try (AbstractJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper()) {
            jdbcHelper.setDataSource(getDataSourceMysql()).init();
            qndDelete(jdbcHelper);
        }
    }

}
