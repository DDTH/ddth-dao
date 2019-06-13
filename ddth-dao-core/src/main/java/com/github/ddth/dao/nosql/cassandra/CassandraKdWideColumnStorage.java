package com.github.ddth.dao.nosql.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.*;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.cql.CqlUtils;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.cql.utils.RetryCallbackResultSet;
import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKdEntryMapper;
import com.github.ddth.dao.nosql.IKdStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.*;

/**
 * Cassandra implementation of {key:document} NoSQL storage.
 *
 * <p>
 * "document" is stored in Cassandra's wide column format {key->{field:value}}, each field's value
 * is serialized to {@code byte[]} before storing. Table schema:
 * </p>
 *
 * <pre>
 * CREATE TABLE mytable (
 *     key          TEXT,
 *     f            TEXT,
 *     v            BLOB,
 *     PRIMARY KEY ((key),f)
 * );
 * </pre>
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class CassandraKdWideColumnStorage extends BaseCassandraStorage implements IKdStorage {

    private final Logger LOGGER = LoggerFactory.getLogger(CassandraKdWideColumnStorage.class);

    private String columnKey = "key", columnField = "f", columnValue = "v";

    private String CQL_DELETE, CQL_DELETE_USING_TIMESTAMP, CQL_SELECT, CQL_INSERT_USING_TIMESTAMP, CQL_COUNT;

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
    public CassandraKdWideColumnStorage setColumnKey(String columnKey) {
        this.columnKey = columnKey;
        return this;
    }

    /**
     * Name of the table column to store "field" name.
     *
     * <p>
     * Note: all tables use the same name for column "field".
     * </p>
     *
     * @return
     */
    public String getColumnField() {
        return columnField;
    }

    /**
     * Name of the table column to store "field" name.
     *
     * <p>
     * Note: all tables use the same name for column "field".
     * </p>
     *
     * @param columnField
     * @return
     */
    public CassandraKdWideColumnStorage setColumnField(String columnField) {
        this.columnField = columnField;
        return this;
    }

    /**
     * Name of the table column to store field "value".
     *
     * <p>
     * Note: all tables use the same name for column "field".
     * </p>
     *
     * @return
     */
    public String getColumnValue() {
        return columnValue;
    }

    /**
     * Name of the table column to store field "value".
     *
     * <p>
     * Note: all tables use the same name for column "field".
     * </p>
     *
     * @param columnValue
     * @return
     */
    public CassandraKdWideColumnStorage setColumnValue(String columnValue) {
        this.columnValue = columnValue;
        return this;
    }

    public CassandraKdWideColumnStorage init() {
        String[] ALL_COLS = { columnKey, columnField, columnValue };
        CQL_DELETE = "DELETE FROM {0} WHERE " + columnKey + "=?";
        CQL_DELETE_USING_TIMESTAMP = "DELETE FROM {0} USING TIMESTAMP {1} WHERE " + columnKey + "=?";
        CQL_SELECT = "SELECT " + StringUtils.join(ALL_COLS, ",") + " FROM {0} WHERE " + columnKey + "=?";
        CQL_INSERT_USING_TIMESTAMP = "INSERT INTO {0} (" + StringUtils.join(ALL_COLS, ",") + ") VALUES (" + StringUtils
                .repeat("?", ",", ALL_COLS.length) + ") USING TIMESTAMP {1}";
        CQL_COUNT = "SELECT DISTINCT count(" + columnKey + ") FROM {0}";
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
    public Map<String, Object> get(String table, String key) {
        String CQL = MessageFormat.format(CQL_SELECT, calcTableName(table));
        ResultSet rs = getSessionManager().execute(CQL, getConsistencyLevelGet(), key);
        if (rs != null) {
            Map<String, byte[]> data = new HashMap<>();
            rs.forEach(row -> {
                String field = row.getString(columnField);
                ByteBuffer value = row.getByteBuffer(columnValue);
                if (value != null) {
                    data.put(field, value.array());
                }
            });
            return bytesMapToDocument(data);
        }
        return null;
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
    public void put(String table, String key, Map<String, Object> doc, IPutCallback<Map<String, Object>> callback) {
        SessionManager sessionManager = getSessionManager();
        String tableName = calcTableName(table);
        long timestamp = System.currentTimeMillis();
        List<Statement<?>> stmList = new ArrayList<>();
        {
            String cqlDelete = MessageFormat.format(CQL_DELETE_USING_TIMESTAMP, tableName, String.valueOf(timestamp));
            Statement<?> stmDelete = sessionManager.bindValues(sessionManager.prepareStatement(cqlDelete), key)
                    .setConsistencyLevel(getConsistencyLevelDelete());
            stmList.add(stmDelete);
        }
        {
            ConsistencyLevel consistencyLevel = getConsistencyLevelPut();
            Map<String, byte[]> data = documentToBytesMap(doc);
            PreparedStatement stmPut = sessionManager.prepareStatement(
                    MessageFormat.format(CQL_INSERT_USING_TIMESTAMP, tableName, String.valueOf(timestamp + 1)));
            data.forEach((f, v) -> {
                stmList.add(sessionManager.bindValues(stmPut, key, f, v).setConsistencyLevel(consistencyLevel));
            });
        }
        BatchStatement batch = CqlUtils.buildBatch(DefaultBatchType.LOGGED, stmList.toArray(new Statement[0]));
        if (isAsyncPut()) {
            try {
                sessionManager.executeAsync(
                        new RetryCallbackResultSet(sessionManager, 1000, DefaultConsistencyLevel.LOCAL_SERIAL, batch) {
                            @Override
                            public void onSuccess(AsyncResultSet result) {
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
                        }, 1000, batch, DefaultConsistencyLevel.LOCAL_SERIAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        } else {
            try {
                sessionManager.execute(batch, DefaultConsistencyLevel.LOCAL_SERIAL);
                if (callback != null) {
                    callback.onSuccess(table, key, doc);
                }
            } catch (Throwable t) {
                doPutErrorCallback(callback, table, key, doc, t);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size(String table) {
        return doCount(MessageFormat.format(CQL_COUNT, calcTableName(table)));
    }

    /**
     * De-serialize byte-array map to "document".
     *
     * @param data
     * @return
     */
    protected Map<String, Object> bytesMapToDocument(Map<String, byte[]> data) {
        if (data == null || data.size() == 0) {
            return null;
        }
        Map<String, Object> doc = new HashMap<>();
        data.forEach((k, v) -> {
            Object value = SerializationUtils.fromByteArrayFst(v);
            if (value != null) {
                doc.put(k, value);
            }
        });
        return doc;
    }

    /**
     * Sereialize "document" to byte-array map.
     *
     * @param doc
     * @return
     */
    @SuppressWarnings("unchecked")
    protected Map<String, byte[]> documentToBytesMap(Map<String, Object> doc) {
        if (doc == null || doc.size() == 0) {
            return Collections.EMPTY_MAP;
        }
        Map<String, byte[]> data = new HashMap<>();
        doc.forEach((k, v) -> {
            byte[] value = SerializationUtils.toByteArrayFst(v);
            if (value != null) {
                data.put(k, value);
            }
        });
        return data;
    }
}
