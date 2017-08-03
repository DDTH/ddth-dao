package com.github.ddth.dao.test.bo.jdbchelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;

public abstract class BaseJdbcHelperTCase {
    protected AbstractJdbcHelper jdbcHelper;
    // private Logger LOGGER =
    // LoggerFactory.getLogger(BaseJdbcHelperTCase.class);

    protected abstract AbstractJdbcHelper buildJdbcHelper() throws SQLException;

    @Before
    public void setup() throws Exception {
        jdbcHelper = buildJdbcHelper();

        try (InputStream is = getClass().getResourceAsStream("jdbchelper_test_initscript.sql")) {
            List<String> lines = IOUtils.readLines(is, "UTF-8");
            try (Connection conn = jdbcHelper.getConnection()) {
                String SQL = "";
                for (String line : lines) {
                    SQL += line;
                    if (line.endsWith(";")) {
                        // LOGGER.info("Executing " + SQL);
                        conn.createStatement().execute(SQL);
                        SQL = "";
                    }
                }
            }
        }

    }

    @After
    public void tearDown() {
        if (jdbcHelper != null) {
            jdbcHelper.destroy();
        }
    }

    @Test
    public void testSelectWhereBindLong() throws Exception {
        final String SQL = "SELECT * FROM tbl_user WHERE id = ?";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, 1L);
            assertNotNull(rows);
            assertEquals(1, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, -1L);
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSelectWhereBindInt() throws Exception {
        final String SQL = "SELECT * FROM tbl_user WHERE yob > ?";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, 1999);
            assertNotNull(rows);
            assertEquals(2, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, 19990);
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSelectWhereBindString() throws Exception {
        final String SQL = "SELECT * FROM tbl_user WHERE username = ?";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, "a");
            assertNotNull(rows);
            assertEquals(1, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, "not_exists");
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }
}
