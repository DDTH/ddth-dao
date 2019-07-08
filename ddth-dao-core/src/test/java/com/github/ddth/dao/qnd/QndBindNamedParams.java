package com.github.ddth.dao.qnd;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.JdbcTemplateJdbcHelper;
import com.github.ddth.dao.jdbc.impl.UniversalRowMapper;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

public class QndBindNamedParams {
    public static void main(String[] args) throws Exception {
        try (Connection conn = DriverManager
                .getConnection("jdbc:mysql://localhost/test?useSSL=false", "test", "test")) {
            DataSource ds = new SingleConnectionDataSource(conn, true);
            try (AbstractJdbcHelper jdbcHelper = new JdbcTemplateJdbcHelper().setDataSource(ds).init()) {
                String sql = "SELECT * FROM tbl_test WHERE id = :id";
                Map<String, Object> bindValues = MapUtils.createMap("id", 1);
                List<Map<String, Object>> dbRows = jdbcHelper
                        .executeSelect(UniversalRowMapper.INSTANCE, sql, bindValues);
                dbRows.forEach(System.out::println);
            }
        }
    }
}
