package com.github.ddth.dao.test.bo.jdbc.genegicjdbcdao;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.github.ddth.dao.jdbc.impl.JdbcTemplateJdbcHelper;
import com.github.ddth.dao.test.TestUtils;
import com.github.ddth.dao.test.bo.jdbc.GenericUserBoRowMapper;
import com.github.ddth.dao.test.bo.jdbc.UserBoJdbcDao;

public class JdbcTemplateGenericJdbcDaoTCase extends BaseGenericJdbcDaoTCase {
    @Override
    protected UserBoJdbcDao buildUserDao() throws SQLException {
        DataSource ds = TestUtils.buildDataSource();
        if (ds == null) {
            return null;
        }

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
