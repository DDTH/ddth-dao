package com.github.ddth.dao.test.bo.jdbc.jdbchelper.exception;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.JdbcTemplateJdbcHelper;

public class JdbcTemplateJdbcHelperTCase extends BaseJdbcHelperTCase {

    @Override
    protected AbstractJdbcHelper buildJdbcHelper() throws SQLException {
        String hostAndPort = System.getProperty("mysql.hostAndPort", "localhost:3306");
        String user = System.getProperty("mysql.user", "travis");
        String password = System.getProperty("mysql.pwd", "");
        String db = System.getProperty("mysql.db", "test");
        String url = "jdbc:mysql://" + hostAndPort + "/" + db
                + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        DataSource ds = new SimpleDriverDataSource(DriverManager.getDriver(url), url, user,
                password);

        JdbcTemplateJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper();
        jdbcHelper.setDataSource(ds);

        jdbcHelper.init();
        return jdbcHelper;
    }

}
