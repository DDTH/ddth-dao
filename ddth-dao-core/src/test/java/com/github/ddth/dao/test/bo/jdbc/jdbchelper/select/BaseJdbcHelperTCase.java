package com.github.ddth.dao.test.bo.jdbc.jdbchelper.select;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.dao.test.bo.jdbc.GenericUserBoRowMapper;
import com.github.ddth.dao.test.bo.jdbc.UserBo;
import com.github.ddth.dao.test.bo.jdbc.UserBoRowMapper;

public abstract class BaseJdbcHelperTCase {

    private final static String TABLE = "tbl_user_jh";

    protected AbstractJdbcHelper jdbcHelper;

    protected abstract AbstractJdbcHelper buildJdbcHelper() throws SQLException;

    @Before
    public void setup() throws Exception {
        jdbcHelper = buildJdbcHelper();
        if (jdbcHelper == null) {
            return;
        }
        try (InputStream is = getClass().getResourceAsStream("/test_initscript.sql")) {
            List<String> lines = IOUtils.readLines(is, "UTF-8");
            try (Connection conn = jdbcHelper.getConnection()) {
                String SQL = "";
                for (String line : lines) {
                    SQL += line;
                    if (line.endsWith(";")) {
                        SQL = SQL.replaceAll("\\$table\\$", TABLE);
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

    /*----------------------------------------------------------------------*/

    @Test
    public void testSelectWhereBindLong() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ?";
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
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > ?";
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
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE username = ?";
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

    @Test
    public void testSelectOneWhereBindLong() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ?";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, 1L);
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, -1L);
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneWhereBindInt() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > ?";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, 1999);
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, 19990);
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneWhereBindString() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE username = ?";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, "a");
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, "not_exists");
            assertNull(row);
        }
    }

    @Test
    public void testSelectWhereBindMulti() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ? OR username = ?";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, 1, "b");
            assertNotNull(rows);
            assertEquals(2, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL, -1.0, "not_exists");
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSelectOneWhereBindMulti() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ? OR username = ?";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, 1, "b");
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> rows = jdbcHelper.executeSelectOne(SQL, -1.0, "not_exists");
            assertNull(rows);
        }
    }

    /*----------------------------------------------------------------------*/
    @Test
    public void testSelectWhereNamedBindLong() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("id", 1L));
            assertNotNull(rows);
            assertEquals(1, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("id", -1L));
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSelectWhereNamedBindInt() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > :yob";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("yob", 1999));
            assertNotNull(rows);
            assertEquals(2, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("yob", 19990));
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSelectWhereNamedBindString() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE username = :username";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("username", "a"));
            assertNotNull(rows);
            assertEquals(1, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("username", "not_exists"));
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSelectOneWhereNamedBindLong() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", 1L));
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", -1L));
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneWhereNamedBindInt() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > :yob";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("yob", 1999));
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("yob", 19990));
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneWhereNamedBindString() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE username = :username";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("username", "a"));
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("username", "not_exists"));
            assertNull(row);
        }
    }

    @Test
    public void testSelectWhereNamedBindMulti() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id OR username = :username";
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("username", "b", "id", 1));
            assertNotNull(rows);
            assertEquals(2, rows.size());
        }
        {
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("username", "not_exists", "id", -1));
            assertNotNull(rows);
            assertEquals(0, rows.size());
        }
    }

    @Test
    public void testSelectOneWhereNamedBindMulti() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id OR username = :username";
        {
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("username", "b", "id", 1));
            assertNotNull(row);
            assertTrue(row.size() > 0);
        }
        {
            Map<String, Object> rows = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", -0.1, "username", "not_exists"));
            assertNull(rows);
        }
    }

    @Test
    public void testSelectOneLabelIndexBind() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT id AS user_id, username AS `user name`, yob FROM " + TABLE
                + " WHERE id=?";
        Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, 1);
        assertNotNull(row);
        assertEquals(1L, row.get("user_id"));
        assertEquals("a", row.get("user name"));
        assertEquals(1999, row.get("yob"));
    }

    @Test
    public void testSelectOneLabelNamedBind() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT id AS user_id, username AS `user name`, yob FROM " + TABLE
                + " WHERE id=:id";
        Map<String, Object> row = jdbcHelper.executeSelectOne(SQL, MapUtils.createMap("id", "1"));
        assertNotNull(row);
        assertEquals(1L, row.get("user_id"));
        assertEquals("a", row.get("user name"));
        assertEquals(1999, row.get("yob"));
    }

    @Test
    public void testSelectBindPrimaryArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE id IN (:id)";
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("id", new int[] { 1, 2, 3 }));
            assertNotNull(rows);
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void testSelectBindObjectArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE id IN (:id)";
            List<Map<String, Object>> rows = jdbcHelper.executeSelect(SQL,
                    MapUtils.createMap("id", new Integer[] { 1, 2, 3 }));
            assertNotNull(rows);
            assertEquals(3, rows.size());
        }
    }

    @Test
    public void testSelectOneBindPrimaryArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE yob IN (:yob) ORDER by yob";
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("yob", new int[] { 1999, 2001 }));
            assertNotNull(row);
            assertEquals(1999, row.get("yob"));

            assertNull(jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", new int[] { 1999, 2001 })));
        }
    }

    @Test
    public void testSelectOneBindObjectArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE username IN (:username)";
            Map<String, Object> row = jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("username", new String[] { "a", "not_exists" }));
            assertNotNull(row);
            assertEquals("a", row.get("username"));

            assertNull(jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", new String[] { "a", "not_exists" })));
        }
    }

    /*----------------------------------------------------------------------*/

    @Test
    public void testSelectBoIndexBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ?";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL, 1);
            assertNotNull(dbRows);
            assertEquals(1, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL, -1L);
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectBoIndexBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > ?";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL, 1999);
            assertNotNull(dbRows);
            assertEquals(2, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL, 19990.1);
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectBoNamedBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("id", 1));
            assertNotNull(dbRows);
            assertEquals(1, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("id", -1L));
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectBoNameBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > :yob";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", 1999));
            assertNotNull(dbRows);
            assertEquals(2, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", "19990.0"));
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectOneBoIndexBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ?";
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL, 1);
            assertNotNull(row);
            assertEquals(1, row.getId());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL, -1L);
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneBoIndexBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > ? ORDER BY yob";
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL, 1999);
            assertNotNull(row);
            assertEquals(2000, row.getYob());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL, 19990.1);
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneBoNamedBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id";
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("id", 3));
            assertNotNull(row);
            assertEquals(3, row.getId());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("id", -1L));
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneBoNameBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > :yob ORDER BY yob DESC";
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", 1999));
            assertNotNull(row);
            assertEquals(2001, row.getYob());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", "19990.0"));
            assertNull(row);
        }
    }

    @Test
    public void testSelectBoBindPrimaryArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE id IN (:id)";
            List<UserBo> rows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("id", new int[] { 2, 3 }));
            assertNotNull(rows);
            assertEquals(2, rows.size());
        }
    }

    @Test
    public void testSelectBoBindObjectArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE username IN (:username)";
            List<UserBo> rows = jdbcHelper.executeSelect(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("username", new String[] { "a" }));
            assertNotNull(rows);
            assertEquals(1, rows.size());
        }
    }

    @Test
    public void testSelectBoOneBindPrimaryArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE yob IN (:yob) ORDER by yob";
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", new int[] { 1999, 2001 }));
            assertNotNull(row);
            assertEquals(1999, row.getYob());

            assertNull(jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", new int[] { 1999, 2001 })));
        }
    }

    @Test
    public void testSelectBoOneBindObjectArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE username IN (:username)";
            UserBo row = jdbcHelper.executeSelectOne(new UserBoRowMapper(), SQL,
                    MapUtils.createMap("username", new String[] { "a", "not_exists" }));
            assertNotNull(row);
            assertEquals("a", row.getUsername());

            assertNull(jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", new String[] { "a", "not_exists" })));
        }
    }

    /*----------------------------------------------------------------------*/

    @Test
    public void testSelectGenericBoMapperIndexBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ?";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL, 1);
            assertNotNull(dbRows);
            assertEquals(1, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL, -1L);
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectGenegicBoMapperIndexBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > ?";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL, 1999);
            assertNotNull(dbRows);
            assertEquals(2, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL,
                    19990.1);
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectGenericBoMapperNamedBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("id", 1));
            assertNotNull(dbRows);
            assertEquals(1, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("id", -1L));
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectGenericBoMapperNameBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > :yob";
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", 1999));
            assertNotNull(dbRows);
            assertEquals(2, dbRows.size());
        }
        {
            List<UserBo> dbRows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", "19990.0"));
            assertNotNull(dbRows);
            assertEquals(0, dbRows.size());
        }
    }

    @Test
    public void testSelectOneGenericBoMapperIndexBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = ?";
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL, 1);
            assertNotNull(row);
            assertEquals(1, row.getId());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL, -1L);
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneGenericBoMapperIndexBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > ? ORDER BY yob";
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL, 1999);
            assertNotNull(row);
            assertEquals(2000, row.getYob());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL, 19990.1);
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneGenericBoMapperNamedBind1() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE id = :id";
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("id", 3));
            assertNotNull(row);
            assertEquals(3, row.getId());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("id", -1L));
            assertNull(row);
        }
    }

    @Test
    public void testSelectOneGenericBoMapperNameBind2() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        final String SQL = "SELECT * FROM " + TABLE + " WHERE yob > :yob ORDER BY yob DESC";
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", 1999));
            assertNotNull(row);
            assertEquals(2001, row.getYob());
        }
        {
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", "19990.0"));
            assertNull(row);
        }
    }

    @Test
    public void testSelectGenericBoMapperBindPrimaryArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE id IN (:id)";
            List<UserBo> rows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("id", new int[] { 2, 3 }));
            assertNotNull(rows);
            assertEquals(2, rows.size());
        }
    }

    @Test
    public void testSelectGenericBoMapperBindObjectArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE username IN (:username)";
            List<UserBo> rows = jdbcHelper.executeSelect(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("username", new String[] { "a" }));
            assertNotNull(rows);
            assertEquals(1, rows.size());
        }
    }

    @Test
    public void testSelectGenericBoMapperOneBindPrimaryArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE yob IN (:yob) ORDER by yob";
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("yob", new int[] { 1999, 2001 }));
            assertNotNull(row);
            assertEquals(1999, row.getYob());

            assertNull(jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", new int[] { 1999, 2001 })));
        }
    }

    @Test
    public void testSelectGenericBoMapperOneBindObjectArray() throws Exception {
        if (jdbcHelper == null) {
            return;
        }
        /*
         * JdbcTemplate does not support binding array yet
         */
        if (jdbcHelper instanceof DdthJdbcHelper) {
            final String SQL = "SELECT * FROM " + TABLE + " WHERE username IN (:username)";
            UserBo row = jdbcHelper.executeSelectOne(new GenericUserBoRowMapper(), SQL,
                    MapUtils.createMap("username", new String[] { "a", "not_exists" }));
            assertNotNull(row);
            assertEquals("a", row.getUsername());

            assertNull(jdbcHelper.executeSelectOne(SQL,
                    MapUtils.createMap("id", new String[] { "a", "not_exists" })));
        }
    }
}
