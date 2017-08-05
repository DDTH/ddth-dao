package com.github.ddth.dao.test.bo.jdbc.jdbchelper.exception;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;

public class DdthJdbcHelperTCase extends BaseJdbcHelperTCase {

    @Override
    protected AbstractJdbcHelper buildJdbcHelper() throws SQLException {
        String hostAndPort = System.getProperty("mysql.hostAndPort", "localhost:3306");
        String user = System.getProperty("mysql.user", "travis");
        String password = System.getProperty("mysql.pwd", "");
        String db = System.getProperty("mysql.db", "test");
        String url = "jdbc:mysql://" + hostAndPort + "/" + db + "?useSSL=false";
        DataSource ds = new SimpleDriverDataSource(DriverManager.getDriver(url), url, user,
                password);

        DdthJdbcHelper jdbcHelper = new DdthJdbcHelper();
        jdbcHelper.setDataSource(ds);

        jdbcHelper.init();
        return jdbcHelper;
    }

}
