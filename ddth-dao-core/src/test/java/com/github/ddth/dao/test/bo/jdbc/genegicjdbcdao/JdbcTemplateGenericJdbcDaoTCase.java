package com.github.ddth.dao.test.bo.jdbc.genegicjdbcdao;

import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import com.github.ddth.dao.jdbc.impl.JdbcTemplateJdbcHelper;
import com.github.ddth.dao.test.bo.jdbc.GenericUserBoRowMapper;
import com.github.ddth.dao.test.bo.jdbc.UserBoJdbcDao;

public class JdbcTemplateGenericJdbcDaoTCase extends BaseGenericJdbcDaoTCase {
    @Override
    protected UserBoJdbcDao buildUserDao() throws SQLException {
        String hostAndPort = System.getProperty("mysql.hostAndPort", "localhost:3306");
        String user = System.getProperty("mysql.user", "travis");
        String password = System.getProperty("mysql.pwd", "");
        String db = System.getProperty("mysql.db", "test");
        String url = "jdbc:mysql://" + hostAndPort + "/" + db
                + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Ho_Chi_Minh";
        DataSource ds = new SimpleDriverDataSource(DriverManager.getDriver(url), url, user,
                password);

        JdbcTemplateJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper();
        jdbcHelper.setDataSource(ds);
        jdbcHelper.init();

        GenericUserBoRowMapper rowMapper = new GenericUserBoRowMapper();

        UserBoJdbcDao userDao = new UserBoJdbcDao();
        userDao.setTableName("tbl_user_gjd").setRowMapper(rowMapper).setJdbcHelper(jdbcHelper);
        userDao.init();
        return userDao;
    }

}
