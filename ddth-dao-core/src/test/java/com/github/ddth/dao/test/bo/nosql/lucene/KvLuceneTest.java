package com.github.ddth.dao.test.bo.nosql.lucene;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.github.ddth.commons.serialization.SerializationException;
import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKvEntryMapper;
import com.github.ddth.dao.nosql.IKvStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import com.github.ddth.dao.nosql.lucene.BaseLuceneStorage;
import com.github.ddth.dao.nosql.lucene.LuceneKvStorage;
import com.github.ddth.dao.test.bo.UserBo;
import com.github.ddth.dao.utils.BoUtils;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class KvLuceneTest extends TestCase {

    public KvLuceneTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(KvLuceneTest.class);
    }

    private final static String SPACE_ID = "test";
    private Directory directory;
    private IKvStorage kvStorage;

    @Before
    public void setUp() throws IOException {
        File dir = new File("./temp");
        FileUtils.deleteQuietly(dir);
        dir.mkdirs();
        directory = FSDirectory.open(dir.toPath());

        LuceneKvStorage storage = new LuceneKvStorage();
        storage.setDirectory(directory).setAsyncWrite(false).init();
        kvStorage = storage;
    }

    @After
    public void tearDown() throws IOException {
        if (kvStorage != null) {
            ((BaseLuceneStorage) kvStorage).close();
        }

        if (directory != null) {
            directory.close();
        }
    }

    @org.junit.Test
    public void testPutExistGet() throws SerializationException, IOException, InterruptedException {
        final long ID = 1;
        final String USERNAME = "username";
        final int YOB = 2018;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
        final String DF_DATE = "yyyy-MM-dd";
        final String DF_TIME = "HH:mm:ss.SSS";
        final String DF_DATETIME = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        final AtomicBoolean success = new AtomicBoolean(false), error = new AtomicBoolean(true);
        final BlockingQueue<Object> QUEUE = new LinkedBlockingQueue<>();
        final String KEY = String.valueOf(ID);
        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(1);
            kvStorage.put(SPACE_ID, KEY, BoUtils.toBytes(bo), new IPutCallback<byte[]>() {
                @Override
                public void onSuccess(String spaceId, String key, byte[] entry) {
                    success.set(true);
                    error.set(false);
                    QUEUE.offer(Boolean.TRUE);
                }

                @Override
                public void onError(String spaceId, String key, byte[] entry, Throwable t) {
                    success.set(false);
                    error.set(true);
                    QUEUE.offer(Boolean.FALSE);
                }
            });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertTrue(kvStorage.keyExists(SPACE_ID, KEY));

        byte[] data = kvStorage.get(SPACE_ID, KEY);
        assertNotNull(data);
        assertTrue(data.length > 0);
        {
            UserBo bo = BoUtils.fromBytes(data, UserBo.class);
            Assert.assertNotNull(bo);
            Assert.assertEquals(ID, bo.getId());
            Assert.assertEquals(USERNAME, bo.getUsername());
            Assert.assertEquals(YOB, bo.getYob());
            Assert.assertEquals(FULLNAME, bo.getFullname());
            Assert.assertTrue(Arrays.equals(BYTEA, bo.getDataBytes()));
            Assert.assertEquals(DateFormatUtils.toString(DATE, DF_DATE),
                    DateFormatUtils.toString(bo.getDataDate(), DF_DATE));
            Assert.assertEquals(DateFormatUtils.toString(DATE, DF_TIME),
                    DateFormatUtils.toString(bo.getDataTime(), DF_TIME));
            Assert.assertEquals(DateFormatUtils.toString(DATE, DF_DATETIME),
                    DateFormatUtils.toString(bo.getDataDatetime(), DF_DATETIME));
        }
    }

    @org.junit.Test
    public void testPutDelete() throws SerializationException, IOException, InterruptedException {
        final long ID = 1;
        final String USERNAME = "username";
        final int YOB = 2018;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
        final AtomicBoolean success = new AtomicBoolean(false), error = new AtomicBoolean(true);
        final BlockingQueue<Object> QUEUE = new LinkedBlockingQueue<>();
        final String KEY = String.valueOf(ID);
        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(1);
            kvStorage.put(SPACE_ID, KEY, BoUtils.toBytes(bo), new IPutCallback<byte[]>() {
                @Override
                public void onSuccess(String spaceId, String key, byte[] entry) {
                    success.set(true);
                    error.set(false);
                    QUEUE.offer(Boolean.TRUE);
                }

                @Override
                public void onError(String spaceId, String key, byte[] entry, Throwable t) {
                    success.set(false);
                    error.set(true);
                    QUEUE.offer(Boolean.FALSE);
                }
            });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertTrue(kvStorage.keyExists(SPACE_ID, KEY));

        {
            success.set(false);
            error.set(true);
            kvStorage.delete(SPACE_ID, KEY, new IDeleteCallback() {
                @Override
                public void onSuccess(String spaceId, String key) {
                    success.set(true);
                    error.set(false);
                    QUEUE.offer(Boolean.TRUE);
                }

                @Override
                public void onError(String spaceId, String key, Throwable t) {
                    success.set(false);
                    error.set(true);
                    QUEUE.offer(Boolean.FALSE);
                }
            });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertFalse(kvStorage.keyExists(SPACE_ID, KEY));

        byte[] data = kvStorage.get(SPACE_ID, KEY);
        assertNull(data);
    }

    @org.junit.Test
    public void testPutExistBo() throws SerializationException, IOException, InterruptedException {
        final long ID = 1;
        final String USERNAME = "username";
        final int YOB = 2018;
        final String FULLNAME = "Mike Wazowski";
        final Date DATE = new Date();
        final byte[] BYTEA = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 0 };
        final String DF_DATE = "yyyy-MM-dd";
        final String DF_TIME = "HH:mm:ss.SSS";
        final String DF_DATETIME = "yyyy-MM-dd'T'HH:mm:ss.SSS";
        final AtomicBoolean success = new AtomicBoolean(false), error = new AtomicBoolean(true);
        final BlockingQueue<Object> QUEUE = new LinkedBlockingQueue<>();
        final String KEY = String.valueOf(ID);
        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(1);
            kvStorage.put(SPACE_ID, KEY, BoUtils.toBytes(bo), new IPutCallback<byte[]>() {
                @Override
                public void onSuccess(String spaceId, String key, byte[] entry) {
                    success.set(true);
                    error.set(false);
                    QUEUE.offer(Boolean.TRUE);
                }

                @Override
                public void onError(String spaceId, String key, byte[] entry, Throwable t) {
                    success.set(false);
                    error.set(true);
                    QUEUE.offer(Boolean.FALSE);
                }
            });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertTrue(kvStorage.keyExists(SPACE_ID, KEY));

        {
            UserBo bo = kvStorage.get(new IKvEntryMapper<UserBo>() {
                @Override
                public UserBo mapEntry(String spaceId, String key, byte[] value) {
                    return BoUtils.fromBytes(value, UserBo.class);
                }
            }, SPACE_ID, KEY);
            assertNotNull(bo);
            Assert.assertNotNull(bo);
            Assert.assertEquals(ID, bo.getId());
            Assert.assertEquals(USERNAME, bo.getUsername());
            Assert.assertEquals(YOB, bo.getYob());
            Assert.assertEquals(FULLNAME, bo.getFullname());
            Assert.assertTrue(Arrays.equals(BYTEA, bo.getDataBytes()));
            Assert.assertEquals(DateFormatUtils.toString(DATE, DF_DATE),
                    DateFormatUtils.toString(bo.getDataDate(), DF_DATE));
            Assert.assertEquals(DateFormatUtils.toString(DATE, DF_TIME),
                    DateFormatUtils.toString(bo.getDataTime(), DF_TIME));
            Assert.assertEquals(DateFormatUtils.toString(DATE, DF_DATETIME),
                    DateFormatUtils.toString(bo.getDataDatetime(), DF_DATETIME));
        }
    }
}
