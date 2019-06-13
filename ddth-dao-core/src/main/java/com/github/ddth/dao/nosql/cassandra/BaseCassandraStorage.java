package com.github.ddth.dao.nosql.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.cql.utils.RetryCallbackResultSet;
import com.github.ddth.dao.nosql.IDeleteCallback;
import com.github.ddth.dao.nosql.IPutCallback;
import com.github.ddth.dao.utils.DaoException;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;

/**
 * Abstract Cassandra implementation of NoSQL storage.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class BaseCassandraStorage implements Closeable {
    private final Logger LOGGER = LoggerFactory.getLogger(BaseCassandraStorage.class);

    private SessionManager sessionManager;
    private String defaultKeyspace;

    private ConsistencyLevel consistencyLevelDelete = DefaultConsistencyLevel.LOCAL_QUORUM;
    private ConsistencyLevel consistencyLevelGet = DefaultConsistencyLevel.LOCAL_QUORUM;
    private ConsistencyLevel consistencyLevelPut = DefaultConsistencyLevel.LOCAL_QUORUM;
    private ConsistencyLevel consistencyLevelCount = DefaultConsistencyLevel.LOCAL_ONE;

    private boolean asyncDelete = false, asyncPut = false;

    /**
     * Getter for {@link #sessionManager}.
     *
     * @return
     */
    protected SessionManager getSessionManager() {
        return sessionManager;
    }

    /**
     * Setter for {@link #sessionManager}.
     *
     * @param sessionManager
     * @return
     */
    public BaseCassandraStorage setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    /**
     * Default keyspace, used when no keyspace is supplied.
     *
     * @return
     */
    public String getDefaultKeyspace() {
        return defaultKeyspace;
    }

    /**
     * Default keyspace, used when no keyspace is supplied.
     *
     * @param defaultKeyspace
     * @return
     */
    public BaseCassandraStorage setDefaultKeyspace(String defaultKeyspace) {
        this.defaultKeyspace = defaultKeyspace;
        return this;
    }

    /**
     * Consistency level for "delete" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_QUORUM}.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelDelete() {
        return consistencyLevelDelete;
    }

    /**
     * Consistency level for "delete" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_QUORUM}.
     *
     * @param consistencyLevelDelete
     * @return
     */
    public BaseCassandraStorage setConsistencyLevelDelete(ConsistencyLevel consistencyLevelDelete) {
        this.consistencyLevelDelete = consistencyLevelDelete;
        return this;
    }

    /**
     * Consistency level for "get" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_QUORUM}.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelGet() {
        return consistencyLevelGet;
    }

    /**
     * Consistency level for "get" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_QUORUM}.
     *
     * @param consistencyLevelGet
     * @return
     */
    public BaseCassandraStorage setConsistencyLevelGet(ConsistencyLevel consistencyLevelGet) {
        this.consistencyLevelGet = consistencyLevelGet;
        return this;
    }

    /**
     * Consistency level for "put" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_QUORUM}.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelPut() {
        return consistencyLevelPut;
    }

    /**
     * Consistency level for "put" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_QUORUM}.
     *
     * @param consistencyLevelPut
     * @return
     */
    public BaseCassandraStorage setConsistencyLevelPut(ConsistencyLevel consistencyLevelPut) {
        this.consistencyLevelPut = consistencyLevelPut;
        return this;
    }

    /**
     * Consistency level for "count" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_ONE}.
     *
     * @return
     */
    public ConsistencyLevel getConsistencyLevelCount() {
        return consistencyLevelCount;
    }

    /**
     * Consistency level for "count" operation. Default value is
     * {@link DefaultConsistencyLevel#LOCAL_ONE}.
     *
     * @param consistencyLevelCount
     * @return
     */
    public BaseCassandraStorage setConsistencyLevelCount(ConsistencyLevel consistencyLevelCount) {
        this.consistencyLevelCount = consistencyLevelCount;
        return this;
    }

    /**
     * "delete" operation should be async or not (default value {@code false})?
     *
     * <p>
     * Async-operation has better performance generally, but may not be completed in case of JVM
     * crash.
     * </p>
     *
     * @return
     */
    public boolean isAsyncDelete() {
        return asyncDelete;
    }

    /**
     * "delete" operation should be async or not (default value {@code false})?
     *
     * <p>
     * Async-operation has better performance generally, but may not be completed in case of JVM
     * crash.
     * </p>
     *
     * @param asyncDelete
     * @return
     */
    public BaseCassandraStorage setAsyncDelete(boolean asyncDelete) {
        this.asyncDelete = asyncDelete;
        return this;
    }

    /**
     * "put" operation should be async or not (default value {@code false})?
     *
     * <p>
     * Async-operation has better performance generally, but may not be completed in case of JVM
     * crash.
     * </p>
     *
     * @return
     */
    public boolean isAsyncPut() {
        return asyncPut;
    }

    /**
     * "put" operation should be async or not (default value {@code false})?
     *
     * <p>
     * Async-operation has better performance generally, but may not be completed in case of JVM
     * crash.
     * </p>
     *
     * @param asyncPut
     * @return
     */
    public BaseCassandraStorage setAsyncPut(boolean asyncPut) {
        this.asyncPut = asyncPut;
        return this;
    }

    public BaseCassandraStorage init() {
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
        // EMPTY
    }

    /*----------------------------------------------------------------------*/

    /**
     * This method prefix {@link #defaultKeyspace} to the target {@code table} if it does not
     * contain keyspace part.
     *
     * @param table
     * @return
     */
    protected String calcTableName(String table) {
        return StringUtils.isBlank(defaultKeyspace) || table.indexOf('.') >= 0 ? table : defaultKeyspace + "." + table;
    }

    /**
     * Execute "count" query.
     *
     * @param cql
     * @return
     * @since 1.0.0
     */
    protected long doCount(String cql) {
        Row row = getSessionManager().executeOne(cql, getConsistencyLevelCount());
        return row != null ? row.getLong(0) : -1;
    }

    /**
     * Execute "delete" operation.
     *
     * @param sessionManager
     * @param cql
     * @param consistencyLevel
     * @param table
     * @param key
     * @param callback
     * @since 1.0.0
     */
    protected void doDelete(SessionManager sessionManager, String cql, ConsistencyLevel consistencyLevel, String table,
            String key, IDeleteCallback callback) {
        if (isAsyncDelete()) {
            doDeleteAsync(sessionManager, cql, getConsistencyLevelDelete(), table, key, callback);
        } else {
            doDeleteSync(sessionManager, cql, getConsistencyLevelDelete(), table, key, callback);
        }
    }

    /**
     * Execute "delete"-sync operation.
     *
     * @param sessionManager
     * @param cql
     * @param consistencyLevel
     * @param table
     * @param key
     * @param callback
     * @since 1.0.0
     */
    private void doDeleteSync(SessionManager sessionManager, String cql, ConsistencyLevel consistencyLevel,
            String table, String key, IDeleteCallback callback) {
        try {
            sessionManager.execute(cql, consistencyLevel, key);
            if (callback != null) {
                callback.onSuccess(table, key);
            }
        } catch (Throwable t) {
            if (callback != null) {
                callback.onError(table, key, t);
            } else {
                throw t instanceof DaoException ? (DaoException) t : new DaoException(t);
            }
        }
    }

    /**
     * Execute "delete"-async operation.
     *
     * @param sessionManager
     * @param cql
     * @param consistencyLevel
     * @param table
     * @param key
     * @param callback
     * @since 1.0.0
     */
    private void doDeleteAsync(SessionManager sessionManager, String cql, ConsistencyLevel consistencyLevel,
            String table, String key, IDeleteCallback callback) {
        try {
            sessionManager.executeAsync(new RetryCallbackResultSet(sessionManager, 1000, consistencyLevel, cql, key) {
                @Override
                public void onSuccess(AsyncResultSet result) {
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
            }, 1000, cql, consistencyLevel, key);
        } catch (InterruptedException e) {
            throw new DaoException(e);
        }
    }

    /**
     * Execute "get" operation.
     *
     * @param sessionManager
     * @param cql
     * @param consistencyLevel
     * @param key
     * @param columnValue
     * @return
     * @since 1.0.0
     */
    protected ByteBuffer doGetBytes(SessionManager sessionManager, String cql, ConsistencyLevel consistencyLevel,
            String key, String columnValue) {
        Row row = sessionManager.executeOne(cql, consistencyLevel, key);
        return row != null ? row.getByteBuffer(columnValue) : null;
    }

    /**
     * @param callback
     * @param table
     * @param key
     * @param doc
     * @param t
     * @param <T>
     * @since 1.0.0
     */
    protected <T> void doPutErrorCallback(IPutCallback<T> callback, String table, String key, T doc, Throwable t) {
        if (callback != null) {
            callback.onError(table, key, doc, t);
        } else {
            throw t instanceof DaoException ? (DaoException) t : new DaoException(t);
        }
    }

    /**
     * Execute "put"-async action.
     *
     * @param sessionManager
     * @param cql
     * @param consistencyLevel
     * @param table
     * @param key
     * @param value
     * @param doc
     * @param callback
     * @param <T>
     * @since 1.0.0
     */
    protected <T> void doPutAsync(SessionManager sessionManager, String cql, ConsistencyLevel consistencyLevel,
            String table, String key, byte[] value, T doc, IPutCallback<T> callback) {
        try {
            sessionManager
                    .executeAsync(new RetryCallbackResultSet(sessionManager, 1000, consistencyLevel, cql, key, value) {
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
                    }, 1000, cql, consistencyLevel, key, value);
        } catch (InterruptedException e) {
            throw new DaoException(e);
        }
    }

    /**
     * Execute "put"-sync action.
     *
     * @param sessionManager
     * @param cql
     * @param consistencyLevel
     * @param table
     * @param key
     * @param value
     * @param doc
     * @param callback
     * @param <T>
     * @since 1.0.0
     */
    protected <T> void doPutSync(SessionManager sessionManager, String cql, ConsistencyLevel consistencyLevel,
            String table, String key, byte[] value, T doc, IPutCallback<T> callback) {
        try {
            sessionManager.execute(cql, consistencyLevel, key, value);
            if (callback != null) {
                callback.onSuccess(table, key, doc);
            }
        } catch (Throwable t) {
            doPutErrorCallback(callback, table, key, doc, t);
        }
    }
}
