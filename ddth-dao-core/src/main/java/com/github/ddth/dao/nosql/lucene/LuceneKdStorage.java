package com.github.ddth.dao.nosql.lucene;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKdEntryMapper;
import com.github.ddth.dao.nosql.IKdStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import com.github.ddth.dao.utils.BoUtils;
import org.apache.lucene.document.*;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Lucene implementation of {key:document} NoSQL storage.
 *
 * <p>
 * Design:
 * <ul>
 * <li>"document" is serialized to {@code byte[]} and stored in a field {@link #FIELD_DATA}.</li>
 * <li>"document"'s scalar fields (string, number, boolean) are indexed in corresponding Lucene
 * fields.</li>
 * </ul>
 * </p>
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class LuceneKdStorage extends BaseLuceneStorage implements IKdStorage {

    public final static String DATETIME_FORMAT = "yyyyMMddHHmmss";

    private final Logger LOGGER = LoggerFactory.getLogger(LuceneKdStorage.class);

    protected final static String FIELD_DATA = "__d";

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String spaceId, String key, IDeleteCallback callback) {
        doDelete(spaceId, key, callback);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean keyExists(String spaceId, String key) throws IOException {
        return get(spaceId, key) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> get(String spaceId, String key) throws IOException {
        Document doc = doGet(spaceId, key);
        BytesRef data = doc != null ? doc.getBinaryValue(FIELD_DATA) : null;
        return data != null ? bytesToDocument(data.bytes) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(IKdEntryMapper<T> mapper, String spaceId, String key) throws IOException {
        Map<String, Object> doc = get(spaceId, key);
        return doc != null ? mapper.mapEntry(spaceId, key, doc) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String spaceId, String key, Map<String, Object> doc, IPutCallback<Map<String, Object>> callback) {
        try {
            byte[] data = documentToBytes(doc);
            Document _doc = createDocument(spaceId, key);
            _doc.add(new StoredField(FIELD_DATA, data));
            doc.forEach((k, v) -> {
                Field field = createIndexField(k, v);
                if (field != null) {
                    _doc.add(field);
                }
            });
            IndexWriter indexWriter = getIndexWriter();
            indexWriter.updateDocument(buildIdTerm(spaceId, key), _doc);
            if (!isAsyncWrite()) {
                indexWriter.commit();
            } else if (getIndexManager().getBackgroundCommitIndexPeriodMs() <= 0) {
                LOGGER.warn("Async-write is enable, autoCommitPeriodMs must be larger than 0!");
            }
            if (callback != null) {
                callback.onSuccess(spaceId, key, doc);
            }
        } catch (Throwable t) {
            if (callback != null) {
                callback.onError(spaceId, key, doc, t);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size(String spaceId) throws IOException {
        return doCount(spaceId);
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

    /**
     * Create index field for a document's field value.
     *
     * <ul>
     * <li>Boolean: indexed as a {@link IntPoint} with value {@code 1=true/0=false}</li>
     * <li>Character: indexed as a {@link StringField}</li>
     * <li>Byte, Short, Integer, Long: indexed as a {@link LongPoint}</li>
     * <li>Float, Double: indexed as a {@link DoublePoint}</li>
     * <li>String: indexed as a {@link StringField} if value contains no space, {@link TextField}
     * otherwise</li>
     * <li>Date: indexed as a {@link StringField}, {@code Date} is converted to {@code String} with
     * format {@link #DATETIME_FORMAT}.</li>
     * </ul>
     *
     * @param key
     * @param value
     * @return
     */
    protected Field createIndexField(String key, Object value) {
        if (key == null || value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return new IntPoint(key, ((Boolean) value).booleanValue() ? 1 : 0);
        }
        if (value instanceof Character) {
            return new StringField(key, value.toString(), Store.NO);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            Number v = (Number) value;
            return new LongPoint(key, v.longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            Number v = (Number) value;
            return new DoublePoint(key, v.doubleValue());
        }
        if (value instanceof Date) {
            return new StringField(key, DateFormatUtils.toString((Date) value, DATETIME_FORMAT), Store.NO);
        }
        if (value instanceof String) {
            String v = value.toString().trim();
            return v.indexOf(' ') >= 0 ? new TextField(key, v, Store.NO) : new StringField(key, v, Store.NO);
        }
        return null;
    }
}
