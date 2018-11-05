package com.github.ddth.dao.test.bo.jdbc.genegicjdbcdao;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.dao.test.TestUtils;
import com.github.ddth.dao.test.bo.jdbc.GenericUserBoRowMapper;
import com.github.ddth.dao.test.bo.jdbc.UserBoJdbcDao;

public class DdthGenericJdbcDaoTCase extends BaseGenericJdbcDaoTCase {
    @Override
    protected UserBoJdbcDao buildUserDao() throws SQLException {
        if (TestUtils.hasSystemPropertyIgnoreCase("skipTestsMySQL")) {
            return null;
        }
        String hostAndPort = System.getProperty("mysql.hostAndPort", "localhost:3306");
        String user = System.getProperty("mysql.user", "root");
        String password = System.getProperty("mysql.pwd", "");
        String db = System.getProperty("mysql.db", "test");
        String url = "jdbc:mysql://" + hostAndPort + "/" + db
                + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false";
        DataSource ds = new SimpleDriverDataSource(DriverManager.getDriver(url), url, user,
                password);

        DdthJdbcHelper jdbcHelper = new DdthJdbcHelper();
        jdbcHelper.setDataSource(ds);
        jdbcHelper.init();

        GenericUserBoRowMapper rowMapper = new GenericUserBoRowMapper();

        UserBoJdbcDao userDao = new UserBoJdbcDao();
        userDao.setTableName("tbl_user_gjd").setRowMapper(rowMapper).setJdbcHelper(jdbcHelper);
        userDao.init();
        return userDao;
    }

}
