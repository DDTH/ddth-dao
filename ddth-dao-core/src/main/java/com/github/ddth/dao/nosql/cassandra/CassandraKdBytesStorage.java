package com.github.ddth.dao.nosql.cassandra;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.cql.utils.RetryFutureCallbackResultSet;
import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKdEntryMapper;
import com.github.ddth.dao.nosql.IKdStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import com.github.ddth.dao.utils.BoUtils;

/**
 * Cassandra implementation of {key:document} NoSQL storage.
 * 
 * <p>
 * "document" is serialized to {@code byte[]} and stored in a BLOB column. Table schema:
 * </p>
 * 
 * <pre>
 * CREATE TABLE mytable (
 *     key          TEXT,
 *     doc          BLOB,
 *     PRIMARY KEY (key)
 * );
 * </pre>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class CassandraKdBytesStorage extends BaseCassandraStorage implements IKdStorage {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraKdBytesStorage.class);

    private String columnKey = "key", columnDocument = "doc";

    private String CQL_DELETE, CQL_SELECT_ONE, CQL_INSERT, CQL_COUNT;

    /**
     * Name of the table column to store "key".
     * 
     * <p>
     * Note: all tables use the same name for column "key".
     * </p>
     * 
     * @return
     */
    public String getColumnKey() {
        return columnKey;
    }

    /**
     * Name of the table column to store "key".
     * 
     * <p>
     * Note: all tables use the same name for column "key".
     * </p>
     * 
     * @param columnKey
     * @return
     */
    public CassandraKdBytesStorage setColumnKey(String columnKey) {
        this.columnKey = columnKey;
        return this;
    }

    /**
     * Name of the table column to store "document".
     * 
     * <p>
     * Note: all tables use the same name for column "document".
     * </p>
     * 
     * @return
     */
    public String getColumnDocument() {
        return columnDocument;
    }

    /**
     * Name of the table column to store "document".
     * 
     * <p>
     * Note: all tables use the same name for column "value".
     * </p>
     * 
     * @param columnDocument
     * @return
     */
    public CassandraKdBytesStorage setColumnDocument(String columnDocument) {
        this.columnDocument = columnDocument;
        return this;
    }

    public CassandraKdBytesStorage init() {
        String[] ALL_COLS = { columnKey, columnDocument };
        CQL_DELETE = "DELETE FROM {0} WHERE " + columnKey + "=?";
        CQL_SELECT_ONE = "SELECT " + StringUtils.join(ALL_COLS, ",") + " FROM {0} WHERE "
                + columnKey + "=?";
        CQL_INSERT = "INSERT INTO {0} (" + StringUtils.join(ALL_COLS, ",") + ") VALUES ("
                + StringUtils.repeat("?", ",", ALL_COLS.length) + ")";
        CQL_COUNT = "SELECT count(" + columnKey + ") FROM {0}";

        return this;
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String table, String key) {
        delete(table, key, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String table, String key, IDeleteCallback callback) {
        SessionManager sessionManager = getSessionManager();
        ConsistencyLevel consistencyLevel = getConsistencyLevelDelete();
        String CQL = MessageFormat.format(CQL_DELETE, calcTableName(table));
        if (isAsyncDelete()) {
            try {
                sessionManager.executeAsync(new RetryFutureCallbackResultSet(sessionManager, 1000,
                        consistencyLevel, CQL, key) {
                    @Override
                    public void onSuccess(ResultSet result) {
                        if (callback != null) {
                            callback.onSuccess(table, key);
                        }
                    }

                    @Override
                    protected void onError(Throwable t) {
                        if (callback != null) {
                            callback.onError(table, key, t);
                        } else {
                            LOGGER.error(t.getMessage());
                        }
                    }
                }, 1000, CQL, consistencyLevel, key);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                sessionManager.execute(CQL, consistencyLevel, key);
                if (callback != null) {
                    callback.onSuccess(table, key);
                }
            } catch (Throwable t) {
                if (callback != null) {
                    callback.onError(table, key, t);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean keyExists(String table, String key) {
        return get(table, key) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> get(String table, String key) {
        String CQL = MessageFormat.format(CQL_SELECT_ONE, calcTableName(table));
        Row row = getSessionManager().executeOne(CQL, getConsistencyLevelGet(), key);
        ByteBuffer data = row != null ? row.getBytes(columnDocument) : null;
        return bytesToDocument(data.array());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(IKdEntryMapper<T> mapper, String table, String key) {
        Map<String, Object> doc = get(table, key);
        return doc != null ? mapper.mapEntry(table, key, doc) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String table, String key, Map<String, Object> doc) {
        put(table, key, doc, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String table, String key, Map<String, Object> doc,
            IPutCallback<Map<String, Object>> callback) {
        SessionManager sessionManager = getSessionManager();
        ConsistencyLevel consistencyLevel = getConsistencyLevelPut();
        String CQL = MessageFormat.format(CQL_INSERT, calcTableName(table));
        byte[] value = documentToBytes(doc);
        if (isAsyncPut()) {
            try {
                sessionManager.executeAsync(new RetryFutureCallbackResultSet(sessionManager, 1000,
                        consistencyLevel, CQL, key, value) {
                    @Override
                    public void onSuccess(ResultSet result) {
                        if (callback != null) {
                            callback.onSuccess(table, key, doc);
                        }
                    }

                    @Override
                    protected void onError(Throwable t) {
                        if (callback != null) {
                            callback.onError(table, key, doc, t);
                        } else {
                            LOGGER.error(t.getMessage());
                        }
                    }
                }, 1000, CQL, consistencyLevel, key, value);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                sessionManager.execute(CQL, getConsistencyLevelPut(), key, value);
                if (callback != null) {
                    callback.onSuccess(table, key, doc);
                }
            } catch (Throwable t) {
                if (callback != null) {
                    callback.onError(table, key, doc, t);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size(String table) {
        String CQL = MessageFormat.format(CQL_COUNT, calcTableName(table));
        Row row = getSessionManager().executeOne(CQL, getConsistencyLevelCount());
        return row != null ? row.getLong(0) : -1;
    }

    /**
     * De-serialize byte array to "document".
     * 
     * @param data
     * @return
     */
    protected Map<String, Object> bytesToDocument(byte[] data) {
        return BoUtils.bytesToDocument(data);
    }

    /**
     * Serialize "document" to byte array.
     * 
     * @param doc
     * @return
     */
    protected byte[] documentToBytes(Map<String, Object> doc) {
        return BoUtils.documentToBytes(doc);
    }
}
