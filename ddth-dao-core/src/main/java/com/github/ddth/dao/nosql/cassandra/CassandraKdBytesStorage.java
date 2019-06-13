package com.github.ddth.dao.nosql.cassandra;

import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKdEntryMapper;
import com.github.ddth.dao.nosql.IKdStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import com.github.ddth.dao.utils.BoUtils;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.text.MessageFormat;
import java.util.Map;

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
    public Map<String, Object> get(String table, String key) {
        String CQL = MessageFormat.format(CQL_SELECT_ONE, calcTableName(table));
        ByteBuffer data = doGetBytes(getSessionManager(), CQL, getConsistencyLevelGet(), key, columnDocument);
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
    public void put(String table, String key, Map<String, Object> doc, IPutCallback<Map<String, Object>> callback) {
        String CQL = MessageFormat.format(CQL_INSERT, calcTableName(table));
        byte[] value = documentToBytes(doc);
        if (isAsyncPut()) {
            doPutAsync(getSessionManager(), CQL, getConsistencyLevelPut(), table, key, value, doc, callback);
        } else {
            doPutSync(getSessionManager(), CQL, getConsistencyLevelPut(), table, key, value, doc, callback);
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
