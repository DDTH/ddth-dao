package com.github.ddth.dao.test.bo.jdbc.jdbchelper.select;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.JdbcTemplateJdbcHelper;
import com.github.ddth.dao.test.TestUtils;

public class JdbcTemplateJdbcHelperTCase extends BaseJdbcHelperTCase {

    @Override
    protected AbstractJdbcHelper buildJdbcHelper() throws SQLException {
        DataSource ds = TestUtils.buildDataSource();
        if (ds == null) {
            return null;
        }

        JdbcTemplateJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper();
        jdbcHelper.setDataSource(ds);

        jdbcHelper.init();
        return jdbcHelper;
    }

}
