package com.github.ddth.dao.test.bo.jdbc.genegicjdbcdao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.dao.BoId;
import com.github.ddth.dao.test.bo.jdbc.UserBo;
import com.github.ddth.dao.test.bo.jdbc.UserBoJdbcDao;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoResult;
import com.github.ddth.dao.utils.DaoResult.DaoOperationStatus;
import com.github.ddth.dao.utils.MissingValueException;

public abstract class BaseGenericJdbcDaoTCase {

    private final static String TABLE = "tbl_user_gjd";

    protected UserBoJdbcDao userDao;
    // private Logger LOGGER =
    // LoggerFactory.getLogger(BaseGenericJdbcDaoTCase.class);

    protected abstract UserBoJdbcDao buildUserDao() throws SQLException;

    @Before
    public void setup() throws Exception {
        userDao = buildUserDao();

        try (InputStream is = getClass().getResourceAsStream("/test_initscript.sql")) {
            List<String> lines = IOUtils.readLines(is, "UTF-8");
            try (Connection conn = userDao.getJdbcHelper().getConnection()) {
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
        if (userDao != null) {
            userDao.destroy();
        }
    }

    /*----------------------------------------------------------------------*/
    @Test
    public void testSelect() throws Exception {
        UserBo bo = userDao.get(new BoId(1));
        assertNotNull(bo);
        assertEquals(1, bo.getId());

        assertNull(userDao.get(new BoId(-1L)));
    }

    @Test
    public void testSelectMulti() throws Exception {
        UserBo[] boList = userDao.get(new BoId(1), new BoId(3), new BoId(5), new BoId(7));
        assertNotNull(boList);
        assertEquals(2, boList.length);
        assertEquals(1, boList[0].getId());
        assertEquals(3, boList[1].getId());
    }

    @Test
    public void testDelete() throws Exception {
        UserBo bo = userDao.get(new BoId(1));
        {
            DaoResult result = userDao.delete(bo);
            assertNotNull(result);
            assertEquals(DaoOperationStatus.SUCCESSFUL, result.getStatus());
        }
        {
            DaoResult result = userDao.delete(bo);
            assertNotNull(result);
            assertEquals(DaoOperationStatus.NOT_FOUND, result.getStatus());
        }
        assertNull(userDao.get(new BoId(1)));
    }

    @Test
    public void testCreate() throws Exception {
        final long ID = 9;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
        final String DF_DATE = "yyyy-MM-dd";
        final String DF_TIME = "HH:mm:ss.SSS";
        final String DF_DATETIME = "yyyy-MM-dd'T'HH:mm:ss.SSS";

        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(1);
            DaoResult result = userDao.create(bo);
            assertNotNull(result);
            assertEquals(DaoOperationStatus.SUCCESSFUL, result.getStatus());
        }
        {
            UserBo bo = userDao.get(new BoId(ID));
            assertNotNull(bo);
            assertEquals(ID, bo.getId());
            assertEquals(USERNAME, bo.getUsername());
            assertEquals(YOB, bo.getYob());
            assertEquals(FULLNAME, bo.getFullname());
            assertTrue(Arrays.equals(BYTEA, bo.getDataBytes()));
            assertEquals(DateFormatUtils.toString(DATE, DF_DATE),
                    DateFormatUtils.toString(bo.getDataDate(), DF_DATE));
            assertEquals(DateFormatUtils.toString(DATE, DF_TIME),
                    DateFormatUtils.toString(bo.getDataTime(), DF_TIME));
            assertEquals(DateFormatUtils.toString(DATE, DF_DATETIME),
                    DateFormatUtils.toString(bo.getDataDatetime(), DF_DATETIME));
        }
    }

    public void testUpdate() throws Exception {
        final long ID = 1;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
        final String DF_DATE = "yyyy-MM-dd";
        final String DF_TIME = "HH:mm:ss.SSS";
        final String DF_DATETIME = "yyyy-MM-dd'T'HH:mm:ss.SSS";

        {
            UserBo bo = userDao.get(new BoId(ID));
            assertNotNull(bo);
            bo.setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA);
            DaoResult result = userDao.update(bo);
            assertNotNull(result);
            assertEquals(DaoOperationStatus.SUCCESSFUL, result.getStatus());
        }
        {
            UserBo bo = userDao.get(new BoId(ID));
            assertNotNull(bo);
            assertEquals(ID, bo.getId());
            assertEquals(USERNAME, bo.getUsername());
            assertEquals(YOB, bo.getYob());
            assertEquals(FULLNAME, bo.getFullname());
            assertTrue(Arrays.equals(BYTEA, bo.getDataBytes()));
            assertEquals(DateFormatUtils.toString(DATE, DF_DATE),
                    DateFormatUtils.toString(bo.getDataDate(), DF_DATE));
            assertEquals(DateFormatUtils.toString(DATE, DF_TIME),
                    DateFormatUtils.toString(bo.getDataTime(), DF_TIME));
            assertEquals(DateFormatUtils.toString(DATE, DF_DATETIME),
                    DateFormatUtils.toString(bo.getDataDatetime(), DF_DATETIME));
        }
    }

    @Test
    public void testInsertDuplicatedKey() throws Exception {
        final long ID = 1;
        final String USERNAME = "username";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(1);
            DaoResult result = userDao.create(bo);
            assertNotNull(result);
            assertEquals(DaoOperationStatus.DUPLICATED_KEY, result.getStatus());
        }
    }

    @Test
    public void testInsertDuplicatedUniqueIndex() throws Exception {
        final long ID = 9;
        final String USERNAME = "a";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(1);
            DaoResult result = userDao.create(bo);
            assertNotNull(result);
            assertEquals(DaoOperationStatus.DUPLICATED_UNIQUE, result.getStatus());
        }
    }

    @Test
    public void testInsertMissingValue() throws Exception {
        final long ID = 9;
        final String USERNAME = "a";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(null);
            Exception e = null;
            try {
                userDao.create(bo);
            } catch (Exception _e) {
                e = _e;
            }
            assertTrue(e instanceof MissingValueException);
        }
    }

    @Test
    public void testInsertInvalidValue() throws Exception {
        final long ID = 9;
        final String USERNAME = "a";
        final int YOB = 2017;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };

        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(-1);
            Exception e = null;
            try {
                userDao.create(bo);
            } catch (Exception _e) {
                e = _e;
            }
            assertTrue(e instanceof DaoException);
        }
    }

    @Test
    public void testUpdateDuplicatedUniqueIndex() throws Exception {
        UserBo bo = userDao.get(new BoId(1));
        assertNotNull(bo);

        {
            bo.setUsername("b");
            DaoResult result = userDao.update(bo);
            assertNotNull(result);
            assertEquals(DaoOperationStatus.DUPLICATED_UNIQUE, result.getStatus());
        }
    }

    @Test
    public void testUpdateMissingValue() throws Exception {
        UserBo bo = userDao.get(new BoId(1));
        assertNotNull(bo);

        {
            bo.setNotNull(null);
            Exception e = null;
            try {
                userDao.update(bo);
            } catch (Exception _e) {
                e = _e;
            }
            assertTrue(e instanceof MissingValueException);
        }
    }

    @Test
    public void testUpdateInvalidValue() throws Exception {
        UserBo bo = userDao.get(new BoId(1));
        assertNotNull(bo);

        {
            bo.setNotNull(null);
            Exception e = null;
            try {
                userDao.update(bo);
            } catch (Exception _e) {
                e = _e;
            }
            assertTrue(e instanceof DaoException);
        }
    }
}