package com.github.ddth.dao.nosql.cassandra;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.cql.CqlUtils;

/**
 * Wide-row, Cassandra-specific NoSQL engine.
 * 
 * <p>
 * This engine assumes tables are created with the following schema:
 * </p>
 * 
 * <pre>
 * CREATE TABLE _table_name_ (
 *     id       text,
 *     key      text,
 *     value    text,
 *     PRIMARY KEY(id,key)
 * ) WITH COMPACT STORAGE;
 * </pre>
 * 
 * <p>
 * where each value is a JSON-encoded object
 * </p>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class WideRowJsonCassandraNosqlEngine extends BaseCassandraNosqlEngine {

    private final static String COL_ID = "id", COL_KEY = "key", COL_VALUE = "value";
    private final static String CQL_DELETE = "DELETE FROM {0} WHERE " + COL_ID + "=?";
    private final static String CQL_LOAD = "SELECT " + COL_ID + "," + COL_KEY + "," + COL_VALUE
            + " FROM {0} WHERE " + COL_ID + "=?";
    private final String CQL_STORE = "INSERT INTO {0} ({1}, {2}, {3}) VALUES (?, ?, ?)";
    private final String CQL_ENTRY_ID_LIST = "SELECT " + COL_ID + " FROM {0}";

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> entryIdList(String tableName) {
        final Set<String> result = new TreeSet<String>();

        final String CQL = MessageFormat.format(CQL_ENTRY_ID_LIST, tableName);
        Session session = getSession();
        List<Row> rows = CqlUtils.execute(session, CQL).all();
        if (rows != null) {
            for (Row row : rows) {
                String id = row.getString(COL_ID);
                result.add(id);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] load(String tableName, String entryId) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(String tableName, String entryId, byte[] data) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String tableName, String entryId) {
        final String CQL = MessageFormat.format(CQL_DELETE, tableName);
        Session session = getSession();
        CqlUtils.executeNonSelect(session, CQL, entryId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Object, Object> loadAsMap(String tableName, String entryId) {
        final String CQL = MessageFormat.format(CQL_LOAD, tableName);
        Session session = getSession();
        List<Row> rows = CqlUtils.execute(session, CQL, entryId).all();
        if (rows == null || rows.size() == 0) {
            // not found
            return null;
        }
        Map<Object, Object> result = new HashMap<Object, Object>();
        for (Row row : rows) {
            String key = row.getString(COL_KEY);
            String value = row.getString(COL_VALUE);
            result.put(key, SerializationUtils.fromJsonString(value, Object.class));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(String tableName, String entryId, Map<Object, Object> data) {
        final String CQL = MessageFormat.format(CQL_STORE, tableName, COL_ID, COL_KEY, COL_VALUE);
        Session session = getSession();
        for (Entry<Object, Object> entry : data.entrySet()) {
            String key = entry.getKey().toString();
            String value = SerializationUtils.toJsonString(entry.getValue());
            CqlUtils.executeNonSelect(session, CQL, entryId, key, value);
        }
    }

}
