package com.github.ddth.dao.test.bo.jdbc.jdbchelper.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.test.TestUtils;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DatabaseVendor;
import com.github.ddth.dao.utils.DbcHelper;
import com.github.ddth.dao.utils.DuplicatedValueException;

public abstract class BaseJdbcHelperTCase {

    private final static String TABLE = "tbl_user_ex";

    protected AbstractJdbcHelper jdbcHelper;

    protected abstract AbstractJdbcHelper buildJdbcHelper() throws SQLException;

    @Before
    public void setup() throws Exception {
        jdbcHelper = buildJdbcHelper();
        if (jdbcHelper == null) {
            System.err.println(
                    "No " + IJdbcHelper.class.getSimpleName() + " is created, tests aborted!");
            return;
        }
        Map<String, String> replacements = new HashMap<String, String>() {
            private static final long serialVersionUID = 1L;
            {
                put("$table$", TABLE);
            }
        };
        try (Connection conn = jdbcHelper.getConnection()) {
            DatabaseVendor dbVendor = DbcHelper.detectDbVendor(conn);
            switch (dbVendor) {
            case MYSQL:
                TestUtils.runSqlScipt(conn, "/test_initscript.mysql.sql", replacements);
                break;
            case POSTGRESQL:
                TestUtils.runSqlScipt(conn, "/test_initscript.pgsql.sql", replacements);
                break;
            default:
                System.err.println("Unknown database vendor: " + dbVendor);
                break;
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
            jdbcHelper.execute(SQL, ID, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, null);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    @Test
    public void testInsertInvalidValue3() {
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
        if (jdbcHelper == null) {
            return;
        }
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
            jdbcHelper.execute(SQL, USERNAME, YOB, FULLNAME, DATE, DATE, DATE, BYTEA, null, ID);
        } catch (Exception _e) {
            e = _e;
        }
        assertNotNull(e);
        assertEquals(DaoException.class, e.getClass());
    }

    @Test
    public void testUpdateInvalidValue3() {
        if (jdbcHelper == null) {
            return;
        }
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
