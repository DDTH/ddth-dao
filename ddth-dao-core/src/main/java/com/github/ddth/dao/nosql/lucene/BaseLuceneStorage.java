package com.github.ddth.dao.nosql.lucene;

import java.io.Closeable;
import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.lucext.directory.IndexManager;

/**
 * Abstract Lucene implementation of NoSQL storage.
 * 
 * <p>
 * Design: 2 special fields
 * <ul>
 * <li>{@link #FIELD_KEY} to store entry's id/key.</li>
 * <li>{@link #FIELD_SPACE_ID} to store entry's space-id.</li>
 * <li>{@link #FIELD_ID} to index term {@code space-id:key}</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class BaseLuceneStorage implements Closeable {

    private final Logger LOGGER = LoggerFactory.getLogger(BaseLuceneStorage.class);

    protected final static String FIELD_KEY = "__k";
    protected final static String FIELD_SPACE_ID = "__s";
    protected final static String FIELD_ID = "__id";

    private Directory directory;
    private IndexWriterConfig indexWriterConfig;
    private IndexManager indexManager;
    private boolean myOwnIndexManager = false;

    private long autoCommitPeriodMs = 0;
    private boolean asyncWrite = false;

    /**
     * Getter for {@link #directory}.
     * 
     * @return
     */
    protected Directory getDirectory() {
        return directory;
    }

    /**
     * Setter for {@link #directory}.
     * 
     * @param directory
     * @return
     */
    public BaseLuceneStorage setDirectory(Directory directory) {
        this.directory = directory;
        return this;
    }

    /**
     * Getter for {@link #indexWriterConfig}.
     * 
     * @return
     */
    protected IndexWriterConfig getIndexWriterConfig() {
        return indexWriterConfig;
    }

    /**
     * Setter for {@link #indexWriterConfig}.
     * 
     * @param indexWriterConfig
     * @return
     */
    public BaseLuceneStorage setIndexWriterConfig(IndexWriterConfig indexWriterConfig) {
        this.indexWriterConfig = indexWriterConfig;
        return this;
    }

    /**
     * Getter for {@link #indexManager}.
     * 
     * @return
     */
    protected IndexManager getIndexManager() {
        return indexManager;
    }

    /**
     * Setter for {@link #indexManager}.
     * 
     * @param indexManager
     * @return
     */
    public BaseLuceneStorage setIndexManager(IndexManager indexManager) {
        if (this.indexManager != null && myOwnIndexManager) {
            this.indexManager.close();
            myOwnIndexManager = false;
        }
        this.indexManager = indexManager;
        return this;
    }

    /**
     * In async-write mode, update operations (delete/put) are performed but not committed, which
     * yields higher update performance but data may be lost in case of JVM crash (default value
     * {@code false})?
     * 
     * <p>
     * Note: {@link #autoCommitPeriodMs} must be set to a positive value to enable async-write.
     * </p>
     * 
     * @return
     * @see #getAutoCommitPeriodMs()
     */
    public boolean isAsyncWrite() {
        return asyncWrite;
    }

    /**
     * In async-write mode, update operations (delete/put) are performed but not committed, which
     * yields higher update performance but data may be lost in case of JVM crash (default value
     * {@code false})?
     * 
     * <p>
     * Note: {@link #autoCommitPeriodMs} must be set to a positive value to enable async-write.
     * </p>
     * 
     * @param asyncWrite
     * @return
     * @see #getAutoCommitPeriodMs()
     */
    public BaseLuceneStorage setAsyncWrite(boolean asyncWrite) {
        this.asyncWrite = asyncWrite;
        return this;
    }

    /**
     * When set to a positive value, data is automatically committed periodically in a background
     * thread.
     * 
     * @return
     */
    public long getAutoCommitPeriodMs() {
        return autoCommitPeriodMs;
    }

    /**
     * When set to a positive value, data is automatically committed periodically in a background
     * thread.
     * 
     * @param autoCommitPeriodMs
     * @return
     */
    public BaseLuceneStorage setAutoCommitPeriodMs(long autoCommitPeriodMs) {
        this.autoCommitPeriodMs = autoCommitPeriodMs;
        return this;
    }

    public BaseLuceneStorage init() throws IOException {
        if (indexManager == null) {
            indexManager = new IndexManager(directory);
            indexManager.init();
            myOwnIndexManager = true;
        }
        if (asyncWrite) {
            if (autoCommitPeriodMs <= 0) {
                LOGGER.warn("Async-write is enable, autoCommitPeriodMs must be larger than 0!");
            } else {
                if (!myOwnIndexManager) {
                    LOGGER.warn("IndexManager instance is not managed by this [" + this + "]");
                }
                indexManager.setBackgroundCommitIndexPeriodMs(autoCommitPeriodMs);
            }
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
    }

    public void destroy() {
        if (indexManager != null && myOwnIndexManager) {
            try {
                indexManager.destroy();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                indexManager = null;
            }
        }
    }

    /*----------------------------------------------------------------------*/
    protected IndexWriter getIndexWriter() {
        return indexManager.getIndexWriter();
    }

    protected IndexSearcher getIndexSearcher() throws IOException {
        return indexManager.getIndexSearcher();
    }

    /**
     * Build query to match entry's space-id and key.
     * 
     * @param spaceId
     * @param key
     * @return
     */
    protected Query buildQuery(String spaceId, String key) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        if (!StringUtils.isBlank(spaceId)) {
            builder.add(new TermQuery(new Term(FIELD_SPACE_ID, spaceId.trim())), Occur.MUST);
        }
        if (!StringUtils.isBlank(key)) {
            builder.add(new TermQuery(new Term(FIELD_KEY, key.trim())), Occur.MUST);
        }
        return builder.build();
    }

    /**
     * Build the "id" term ({@code id="spaceId:key"}
     * 
     * @param spaceId
     * @param key
     * @return
     */
    protected Term buildIdTerm(String spaceId, String key) {
        return new Term(FIELD_ID, spaceId.trim() + ":" + key.trim());
    }

    /**
     * Create a {@link Document}, pre-filled with space-id and key fields.
     * 
     * @param spaceId
     * @param key
     * @return
     */
    protected Document createDocument(String spaceId, String key) {
        Document doc = new Document();
        doc.add(new StringField(FIELD_SPACE_ID, spaceId.trim(), Store.YES));
        doc.add(new StringField(FIELD_KEY, key.trim(), Store.YES));
        doc.add(new StringField(FIELD_ID, spaceId.trim() + ":" + key.trim(), Store.NO));
        return doc;
    }

    /*----------------------------------------------------------------------*/
    /**
     * Delete a document, identified by {@code spaceId:key}, from index.
     * 
     * @param spaceId
     * @param key
     * @throws IOException
     */
    protected void doDelete(String spaceId, String key) throws IOException {
        Term termDelete = buildIdTerm(spaceId, key);
        IndexWriter indexWriter = getIndexWriter();
        indexWriter.deleteDocuments(termDelete);
        if (!isAsyncWrite()) {
            indexWriter.commit();
        } else if (getIndexManager().getBackgroundCommitIndexPeriodMs() <= 0) {
            LOGGER.warn("Async-write is enable, autoCommitPeriodMs must be larger than 0!");
        }
    }

    /**
     * Fetch a document, identified by {@code spaceId:key}, from index.
     * 
     * @param spaceId
     * @param key
     * @return
     * @throws IOException
     */
    protected Document doGet(String spaceId, String key) throws IOException {
        Term termGet = buildIdTerm(spaceId, key);
        IndexSearcher indexSearcher = getIndexSearcher();
        TopDocs result = indexSearcher.search(new TermQuery(termGet), 1);
        ScoreDoc[] hits = result != null ? result.scoreDocs : null;
        Document doc = hits != null && hits.length > 0 ? indexSearcher.doc(hits[0].doc) : null;
        return doc;
    }

    /**
     * Count number of documents within a space.
     * 
     * @param spaceId
     * @return
     * @throws IOException
     */
    protected long doCount(String spaceId) throws IOException {
        TopDocs result = getIndexSearcher().search(buildQuery(spaceId, null), 1);
        return result != null ? result.totalHits : -1;
    }
}
