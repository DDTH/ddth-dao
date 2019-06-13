package com.github.ddth.dao.nosql.lucene;

import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IKvEntryMapper;
import com.github.ddth.dao.nosql.IKvStorage;
import com.github.ddth.dao.nosql.IPutCallback;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Lucene implementation of {key:value} NoSQL storage.
 *
 * <p>
 * Design: value is stored in field {@link #FIELD_VALUE}.
 * </p>
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class LuceneKvStorage extends BaseLuceneStorage implements IKvStorage {

    private final Logger LOGGER = LoggerFactory.getLogger(LuceneKvStorage.class);

    protected final static String FIELD_VALUE = "__v";

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
    public byte[] get(String spaceId, String key) throws IOException {
        Document doc = doGet(spaceId, key);
        BytesRef data = doc != null ? doc.getBinaryValue(FIELD_VALUE) : null;
        return data != null ? data.bytes : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(IKvEntryMapper<T> mapper, String spaceId, String key) throws IOException {
        byte[] data = get(spaceId, key);
        return data != null ? mapper.mapEntry(spaceId, key, data) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String spaceId, String key, byte[] value, IPutCallback<byte[]> callback) {
        try {
            Document _doc = createDocument(spaceId, key);
            _doc.add(new StoredField(FIELD_VALUE, value));
            IndexWriter indexWriter = getIndexWriter();
            indexWriter.updateDocument(buildIdTerm(spaceId, key), _doc);
            if (!isAsyncWrite()) {
                indexWriter.commit();
            } else if (getIndexManager().getBackgroundCommitIndexPeriodMs() <= 0) {
                LOGGER.warn("Async-write is enable, autoCommitPeriodMs must be larger than 0!");
            }
            if (callback != null) {
                callback.onSuccess(spaceId, key, value);
            }
        } catch (Throwable t) {
            if (callback != null) {
                callback.onError(spaceId, key, value, t);
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
}
