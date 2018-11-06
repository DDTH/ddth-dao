package com.github.ddth.dao.test.bo.jdbc.jdbchelper.exception;

import java.sql.SQLException;

import javax.sql.DataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.dao.test.TestUtils;

public class DdthJdbcHelperTCase extends BaseJdbcHelperTCase {

    @Override
    protected AbstractJdbcHelper buildJdbcHelper() throws SQLException {
        DataSource ds = TestUtils.buildDataSource();
        if (ds == null) {
            return null;
        }

        DdthJdbcHelper jdbcHelper = new DdthJdbcHelper();
        jdbcHelper.setDataSource(ds);

        jdbcHelper.init();
        return jdbcHelper;
    }

}
