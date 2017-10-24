package com.github.ddth.dao.test.bo.jdbc.jdbchelper.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DuplicatedValueException;

public abstract class BaseJdbcHelperTCase {

    private final static String TABLE = "tbl_user_ex";

    protected AbstractJdbcHelper jdbcHelper;
    // private Logger LOGGER =
    // LoggerFactory.getLogger(BaseJdbcHelperTCase.class);

    protected abstract AbstractJdbcHelper buildJdbcHelper() throws SQLException;

    @Before
    public void setup() throws Exception {
        jdbcHelper = buildJdbcHelper();

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
    public void testTableNotFound() throws Exception {
        final String SQL = "SELECT * FROM table_not_exists";
        Exception e = null;
        try {
            jdbcHelper.executeSelect(SQL);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    @Test
    public void testColumnNotFound() throws Exception {
        final String SQL = "SELECT col_not_exists FROM " + TABLE;
        Exception e = null;
        try {
            jdbcHelper.executeSelect(SQL);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    @Test
    public void testInvalidSyntax() throws Exception {
        final String SQL = "SENECT * FROM " + TABLE;
        Exception e = null;
        try {
            jdbcHelper.executeSelect(SQL);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    /*----------------------------------------------------------------------*/

    private final String[] COLS_INSERT = { "id", "username", "yob", "fullname", "data_date",
            "data_time", "data_datetime", "data_bin", "data_notnull" };

    @Test
    public void testInsertDuplicatedPK() {
        final long ID = 1;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "INSERT INTO " + TABLE + "(" + StringUtils.join(COLS_INSERT, ",")
                + ") VALUES (" + StringUtils.repeat("?", ",", COLS_INSERT.length) + ")";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, 0);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DuplicatedValueException.class, e.getClass());
    }

    @Test
    public void testInsertDuplicatedUniqueIndex() {
        final long ID = 9;
        final String USERNAME = "a";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "INSERT INTO " + TABLE + "(" + StringUtils.join(COLS_INSERT, ",")
                + ") VALUES (" + StringUtils.repeat("?", ",", COLS_INSERT.length) + ")";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, 0);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DuplicatedValueException.class, e.getClass());
    }

    // @Test
    // public void testInsertMissingValue() {
    // final long ID = 9;
    // final String USERNAME = "username";
    // final int YOB = 2017;
    // final String FULLNAME = "Mike Wazowski";
    // final Date DATE = new Date();
    // final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    //
    // final String SQL = "INSERT INTO " + TABLE + "(" + StringUtils.join(COLS_INSERT, ",")
    // + ") VALUES (" + StringUtils.repeat("?", ",", COLS_INSERT.length) + ")";
    // Exception e = null;
    // try {
    // jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, null);
    // } catch (Exception _e) {
    // e = _e;
    // }
    // assertNotNull(e);
    // assertEquals(MissingValueException.class, e.getClass());
    // }

    @Test
    public void testInsertInvalidValue1() {
        final long ID = 9;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "INSERT INTO " + TABLE + "(" + StringUtils.join(COLS_INSERT, ",")
                + ") VALUES (" + StringUtils.repeat("?", ",", COLS_INSERT.length) + ")";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, "null");
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
        // e.printStackTrace();
    }

    @Test
    public void testInsertInvalidValue2() {
        final long ID = 9;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "INSERT INTO " + TABLE + "(" + StringUtils.join(COLS_INSERT, ",")
                + ") VALUES (" + StringUtils.repeat("?", ",", COLS_INSERT.length) + ")";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, -1);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    @Test
    public void testInsertInvalidValue3() {
        final long ID = 9;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "INSERT INTO " + TABLE + "(" + StringUtils.join(COLS_INSERT, ",")
                + ") VALUES (" + StringUtils.repeat("?", ",", COLS_INSERT.length) + ")";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA,
                    Long.MAX_VALUE);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }
    /*----------------------------------------------------------------------*/

    private final String[] COLS_UPDATE_ALL = { "id=?", "username=?", "yob=?", "fullname=?",
            "data_date=?", "data_time=?", "data_datetime=?", "data_bin=?", "data_notnull=?" };
    private final String[] COLS_UPDATE = { "username=?", "yob=?", "fullname=?", "data_date=?",
            "data_time=?", "data_datetime=?", "data_bin=?", "data_notnull=?" };

    @Test
    public void testUpdateDuplicatedPK() {
        final long ID = 2;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "UPDATE " + TABLE + " SET " + StringUtils.join(COLS_UPDATE_ALL, ",")
                + " WHERE id=?";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, 0, 1);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DuplicatedValueException.class, e.getClass());
    }

    @Test
    public void testUpdateDuplicatedUniqueIndex() {
        final long ID = 1;
        final String USERNAME = "b";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "UPDATE " + TABLE + " SET " + StringUtils.join(COLS_UPDATE, ",")
                + " WHERE id=?";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, 0, ID);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DuplicatedValueException.class, e.getClass());
    }

    // @Test
    // public void testUpdateMissingValue() {
    // final long ID = 1;
    // final String USERNAME = "a";
    // final int YOB = 2017;
    // final String FULLNAME = "Mike Wazowski";
    // final Date DATE = new Date();
    // final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
    //
    // final String SQL = "UPDATE " + TABLE + " SET " + StringUtils.join(COLS_UPDATE, ",")
    // + " WHERE id=?";
    // Exception e = null;
    // try {
    // jdbcHelper.execute(SQL, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, null, ID);
    // } catch (Exception _e) {
    // e = _e;
    // }
    // assertNotNull(e);
    // assertEquals(MissingValueException.class, e.getClass());
    // }

    @Test
    public void testUpdateInvalidValue1() {
        final long ID = 1;
        final String USERNAME = "a";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "UPDATE " + TABLE + " SET " + StringUtils.join(COLS_UPDATE, ",")
                + " WHERE id=?";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, 'a', ID);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    @Test
    public void testUpdateInvalidValue2() {
        final long ID = 1;
        final String USERNAME = "a";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "UPDATE " + TABLE + " SET " + StringUtils.join(COLS_UPDATE, ",")
                + " WHERE id=?";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, -1, ID);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    @Test
    public void testUpdateInvalidValue3() {
        final long ID = 1;
        final String USERNAME = "a";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        final String SQL = "UPDATE " + TABLE + " SET " + StringUtils.join(COLS_UPDATE, ",")
                + " WHERE id=?";
        Exception e = null;
        try {
            jdbcHelper.execute(SQL, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA,
                    Long.MAX_VALUE, ID);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    /*----------------------------------------------------------------------*/
    // @Test
    // public void testDeadlock() throws Exception {
    // try (Connection conn1 = jdbcHelper.getDataSource().getConnection()) {
    // conn1.setAutoCommit(false);
    // // conn1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    // conn1.createStatement().executeQuery("SELECT * FROM " + TABLE + " WHERE
    // yob=2000");
    // conn1.createStatement().execute("UPDATE " + TABLE + " SET yob=2000 WHERE
    // yob<>2000");
    //
    // try (Connection conn2 = jdbcHelper.getDataSource().getConnection()) {
    // conn2.setAutoCommit(false);
    // // conn2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    // jdbcHelper.executeSelect(conn2, "SELECT * FROM " + TABLE + " WHERE
    // yob=1999");
    // jdbcHelper.execute(conn2, "UPDATE " + TABLE + " SET yob=1999 WHERE
    // yob<>1999");
    // }
    // }
    // }
}
