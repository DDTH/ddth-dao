package com.github.ddth.dao.nosql.cassandra;

import java.nio.ByteBuffer;
import java.text.MessageFormat;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.cql.utils.RetryFutureCallbackResultSet;
import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKvEntryMapper;
import com.github.ddth.dao.nosql.IKvStorage;
import com.github.ddth.dao.nosql.IPutCallback;

/**
 * Cassandra implementation of {key:value} NoSQL storage.
 * 
 * <p>
 * Table schema:
 * </p>
 * 
 * <pre>
 * CREATE TABLE mytable (
 *     key          TEXT,
 *     value        BLOB,
 *     PRIMARY KEY (key)
 * );
 * </pre>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class CassandraKvStorage extends BaseCassandraStorage implements IKvStorage {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraKvStorage.class);

    private String columnKey = "key", columnValue = "value";

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
    public CassandraKvStorage setColumnKey(String columnKey) {
        this.columnKey = columnKey;
        return this;
    }

    /**
     * Name of the table column to store "value".
     * 
     * <p>
     * Note: all tables use the same name for column "value".
     * </p>
     * 
     * @return
     */
    public String getColumnValue() {
        return columnValue;
    }

    /**
     * Name of the table column to store "value".
     * 
     * <p>
     * Note: all tables use the same name for column "value".
     * </p>
     * 
     * @param columnValue
     * @return
     */
    public CassandraKvStorage setColumnValue(String columnValue) {
        this.columnValue = columnValue;
        return this;
    }

    public CassandraKvStorage init() {
        String[] ALL_COLS = { columnKey, columnValue };
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
    public byte[] get(String table, String key) {
        String CQL = MessageFormat.format(CQL_SELECT_ONE, calcTableName(table));
        Row row = getSessionManager().executeOne(CQL, getConsistencyLevelGet(), key);
        ByteBuffer data = row != null ? row.getBytes(columnValue) : null;
        return data != null ? data.array() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(IKvEntryMapper<T> mapper, String table, String key) {
        byte[] data = get(table, key);
        return data != null ? mapper.mapEntry(table, key, data) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String table, String key, byte[] value) {
        put(table, key, value, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String table, String key, byte[] value, IPutCallback<byte[]> callback) {
        SessionManager sessionManager = getSessionManager();
        ConsistencyLevel consistencyLevel = getConsistencyLevelPut();
        String CQL = MessageFormat.format(CQL_INSERT, calcTableName(table));
        if (isAsyncPut()) {
            try {
                sessionManager.executeAsync(new RetryFutureCallbackResultSet(sessionManager, 1000,
                        consistencyLevel, CQL, key, value) {
                    @Override
                    public void onSuccess(ResultSet result) {
                        if (callback != null) {
                            callback.onSuccess(table, key, value);
                        }
                    }

                    @Override
                    protected void onError(Throwable t) {
                        if (callback != null) {
                            callback.onError(table, key, value, t);
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
                    callback.onSuccess(table, key, value);
                }
            } catch (Throwable t) {
                if (callback != null) {
                    callback.onError(table, key, value, t);
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
}
