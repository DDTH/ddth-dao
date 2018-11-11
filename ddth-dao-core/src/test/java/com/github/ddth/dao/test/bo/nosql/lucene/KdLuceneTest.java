package com.github.ddth.dao.test.bo.nosql.lucene;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import com.github.ddth.commons.serialization.SerializationException;
import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.commons.utils.ReflectionUtils;
import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKdEntryMapper;
import com.github.ddth.dao.nosql.IKdStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import com.github.ddth.dao.nosql.lucene.BaseLuceneStorage;
import com.github.ddth.dao.nosql.lucene.LuceneKdStorage;
import com.github.ddth.dao.test.bo.UserBo;
import com.github.ddth.lucext.directory.IndexManager;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class KdLuceneTest extends TestCase {

    public KdLuceneTest(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(KdLuceneTest.class);
    }

    private final static String SPACE_ID = "test";
    private Directory directory;
    private IKdStorage kdStorage;

    @Before
    public void setUp() throws IOException {
        File dir = new File("./temp");
        FileUtils.deleteQuietly(dir);
        dir.mkdirs();
        directory = FSDirectory.open(dir.toPath());

        LuceneKdStorage storage = new LuceneKdStorage();
        storage.setDirectory(directory).setAsyncWrite(false).init();
        kdStorage = storage;
    }

    @After
    public void tearDown() throws IOException {
        if (kdStorage != null) {
            ((BaseLuceneStorage) kdStorage).close();
        }

        if (directory != null) {
            directory.close();
        }
    }

    private void verifyBo(UserBo bo, long ID, String USERNAME, int YOB, String FULLNAME,
            byte[] BYTEA, Date DATE) {
        final String DF_DATE = "yyyy-MM-dd";
        final String DF_TIME = "HH:mm:ss.SSS";
        final String DF_DATETIME = "yyyy-MM-dd'T'HH:mm:ss.SSS";

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

    @org.junit.Test
    public void testPutExistGet() throws SerializationException, IOException, InterruptedException {
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
            kdStorage.put(SPACE_ID, KEY, bo.getAttributes(),
                    new IPutCallback<Map<String, Object>>() {
                        @Override
                        public void onSuccess(String spaceId, String key,
                                Map<String, Object> entry) {
                            success.set(true);
                            error.set(false);
                            QUEUE.offer(Boolean.TRUE);
                        }

                        @Override
                        public void onError(String spaceId, String key, Map<String, Object> entry,
                                Throwable t) {
                            success.set(false);
                            error.set(true);
                            QUEUE.offer(Boolean.FALSE);
                        }
                    });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertTrue(kdStorage.keyExists(SPACE_ID, KEY));

        Map<String, Object> data = kdStorage.get(SPACE_ID, KEY);
        assertNotNull(data);
        assertTrue(data.size() > 0);
        {
            UserBo bo = new UserBo();
            bo.setAttributes(data);
            verifyBo(bo, ID, USERNAME, YOB, FULLNAME, BYTEA, DATE);
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
            kdStorage.put(SPACE_ID, KEY, bo.getAttributes(),
                    new IPutCallback<Map<String, Object>>() {
                        @Override
                        public void onSuccess(String spaceId, String key,
                                Map<String, Object> entry) {
                            success.set(true);
                            error.set(false);
                            QUEUE.offer(Boolean.TRUE);
                        }

                        @Override
                        public void onError(String spaceId, String key, Map<String, Object> entry,
                                Throwable t) {
                            success.set(false);
                            error.set(true);
                            QUEUE.offer(Boolean.FALSE);
                        }
                    });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertTrue(kdStorage.keyExists(SPACE_ID, KEY));

        {
            success.set(false);
            error.set(true);
            kdStorage.delete(SPACE_ID, KEY, new IDeleteCallback() {

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
        Assert.assertFalse(kdStorage.keyExists(SPACE_ID, KEY));

        Map<String, Object> data = kdStorage.get(SPACE_ID, KEY);
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
        final AtomicBoolean success = new AtomicBoolean(false), error = new AtomicBoolean(true);
        final BlockingQueue<Object> QUEUE = new LinkedBlockingQueue<>();
        final String KEY = String.valueOf(ID);
        {
            UserBo bo = new UserBo();
            bo.setId(ID).setUsername(USERNAME).setYob(YOB).setFullname(FULLNAME).setDataDate(DATE)
                    .setDataTime(DATE).setDataDatetime(DATE).setDataBytes(BYTEA).setNotNull(1);
            kdStorage.put(SPACE_ID, KEY, bo.getAttributes(),
                    new IPutCallback<Map<String, Object>>() {
                        @Override
                        public void onSuccess(String spaceId, String key,
                                Map<String, Object> entry) {
                            success.set(true);
                            error.set(false);
                            QUEUE.offer(Boolean.TRUE);
                        }

                        @Override
                        public void onError(String spaceId, String key, Map<String, Object> entry,
                                Throwable t) {
                            success.set(false);
                            error.set(true);
                            QUEUE.offer(Boolean.FALSE);
                        }
                    });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertTrue(kdStorage.keyExists(SPACE_ID, KEY));

        {
            UserBo bo = kdStorage.get(new IKdEntryMapper<UserBo>() {
                @Override
                public UserBo mapEntry(String spaceId, String key, Map<String, Object> value) {
                    UserBo bo = new UserBo();
                    bo.setAttributes(value);
                    return bo;
                }

            }, SPACE_ID, KEY);
            verifyBo(bo, ID, USERNAME, YOB, FULLNAME, BYTEA, DATE);
        }
    }

    @org.junit.Test
    public void testPutSearch() throws SerializationException, IOException, InterruptedException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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
            kdStorage.put(SPACE_ID, KEY, bo.getAttributes(),
                    new IPutCallback<Map<String, Object>>() {
                        @Override
                        public void onSuccess(String spaceId, String key,
                                Map<String, Object> entry) {
                            success.set(true);
                            error.set(false);
                            QUEUE.offer(Boolean.TRUE);
                        }

                        @Override
                        public void onError(String spaceId, String key, Map<String, Object> entry,
                                Throwable t) {
                            success.set(false);
                            error.set(true);
                            QUEUE.offer(Boolean.FALSE);
                        }
                    });
        }

        Assert.assertNotNull(QUEUE.poll(1000, TimeUnit.MILLISECONDS));
        Assert.assertTrue(success.get());
        Assert.assertFalse(error.get());
        Assert.assertTrue(kdStorage.keyExists(SPACE_ID, KEY));

        Method m = ReflectionUtils.getMethod("getIndexManager", BaseLuceneStorage.class);
        m.setAccessible(true);
        IndexManager indexManager = (IndexManager) m.invoke(kdStorage);
        IndexSearcher indexSearcher = indexManager.getIndexSearcher();

        {
            TopDocs result = indexSearcher.search(LongPoint.newExactQuery(UserBo.ATTR_ID, ID), 1);
            Assert.assertEquals(1, result.totalHits);
        }
        {
            TopDocs result = indexSearcher
                    .search(new TermQuery(new Term(UserBo.ATTR_USERNAME, USERNAME)), 1);
            Assert.assertEquals(1, result.totalHits);
        }
        {
            TopDocs result = indexSearcher.search(LongPoint.newExactQuery(UserBo.ATTR_YOB, YOB), 1);
            Assert.assertEquals(1, result.totalHits);
        }
        {
            QueryBuilder qb = new QueryBuilder(new StandardAnalyzer());
            Query q = qb.createBooleanQuery(UserBo.ATTR_FULLNAME, FULLNAME, Occur.SHOULD);
            TopDocs result = indexSearcher.search(q, 1);
            Assert.assertEquals(1, result.totalHits);
        }
        {
            String term = DateFormatUtils.toString(DATE, LuceneKdStorage.DATETIME_FORMAT);
            TopDocs result = indexSearcher.search(new TermQuery(new Term(UserBo.ATTR_DATE, term)),
                    1);
            Assert.assertEquals(1, result.totalHits);
        }
        {
            String term = DateFormatUtils.toString(DATE, LuceneKdStorage.DATETIME_FORMAT);
            TopDocs result = indexSearcher
                    .search(new TermQuery(new Term(UserBo.ATTR_DATETIME, term)), 1);
            Assert.assertEquals(1, result.totalHits);
        }
        {
            String term = DateFormatUtils.toString(DATE, LuceneKdStorage.DATETIME_FORMAT);
            TopDocs result = indexSearcher.search(new TermQuery(new Term(UserBo.ATTR_TIME, term)),
                    1);
            Assert.assertEquals(1, result.totalHits);
        }
    }
}
