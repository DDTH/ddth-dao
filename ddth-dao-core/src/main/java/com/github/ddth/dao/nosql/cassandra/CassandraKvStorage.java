package com.github.ddth.dao.nosql.cassandra;

import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKvEntryMapper;
import com.github.ddth.dao.nosql.IKvStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.text.MessageFormat;

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
        CQL_SELECT_ONE = "SELECT " + StringUtils.join(ALL_COLS, ",") + " FROM {0} WHERE " + columnKey + "=?";
        CQL_INSERT = "INSERT INTO {0} (" + StringUtils.join(ALL_COLS, ",") + ") VALUES (" + StringUtils
                .repeat("?", ",", ALL_COLS.length) + ")";
        CQL_COUNT = "SELECT count(" + columnKey + ") FROM {0}";
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String table, String key, IDeleteCallback callback) {
        String CQL = MessageFormat.format(CQL_DELETE, calcTableName(table));
        doDelete(getSessionManager(), CQL, getConsistencyLevelDelete(), table, key, callback);
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
        ByteBuffer data = doGetBytes(getSessionManager(), CQL, getConsistencyLevelGet(), key, columnValue);
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
    public void put(String table, String key, byte[] value, IPutCallback<byte[]> callback) {
        String CQL = MessageFormat.format(CQL_INSERT, calcTableName(table));
        if (isAsyncPut()) {
            doPutAsync(getSessionManager(), CQL, getConsistencyLevelPut(), table, key, value, value, callback);
        } else {
            doPutSync(getSessionManager(), CQL, getConsistencyLevelPut(), table, key, value, value, callback);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size(String table) {
        return doCount(MessageFormat.format(CQL_COUNT, calcTableName(table)));
    }
}
