package com.github.ddth.dao.nosql.cassandra;

import java.io.Closeable;

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.ConsistencyLevel;
import com.github.ddth.cql.SessionManager;

/**
 * Abstract Cassandra implementation of NoSQL storage.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class BaseCassandraStorage implements Closeable {

    private SessionManager sessionManager;
    private String defaultKeyspace;

    private ConsistencyLevel consistencyLevelDelete = ConsistencyLevel.LOCAL_QUORUM;
    private ConsistencyLevel consistencyLevelGet = ConsistencyLevel.LOCAL_QUORUM;
    private ConsistencyLevel consistencyLevelPut = ConsistencyLevel.LOCAL_QUORUM;
    private ConsistencyLevel consistencyLevelCount = ConsistencyLevel.LOCAL_ONE;

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
     * {@link ConsistencyLevel#LOCAL_QUORUM}.
     * 
     * @return
     */
    public ConsistencyLevel getConsistencyLevelDelete() {
        return consistencyLevelDelete;
    }

    /**
     * Consistency level for "delete" operation. Default value is
     * {@link ConsistencyLevel#LOCAL_QUORUM}.
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
     * {@link ConsistencyLevel#LOCAL_QUORUM}.
     * 
     * @return
     */
    public ConsistencyLevel getConsistencyLevelGet() {
        return consistencyLevelGet;
    }

    /**
     * Consistency level for "get" operation. Default value is
     * {@link ConsistencyLevel#LOCAL_QUORUM}.
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
     * {@link ConsistencyLevel#LOCAL_QUORUM}.
     * 
     * @return
     */
    public ConsistencyLevel getConsistencyLevelPut() {
        return consistencyLevelPut;
    }

    /**
     * Consistency level for "put" operation. Default value is
     * {@link ConsistencyLevel#LOCAL_QUORUM}.
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
     * {@link ConsistencyLevel#LOCAL_ONE}.
     * 
     * @return
     */
    public ConsistencyLevel getConsistencyLevelCount() {
        return consistencyLevelCount;
    }

    /**
     * Consistency level for "count" operation. Default value is
     * {@link ConsistencyLevel#LOCAL_ONE}.
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
        return StringUtils.isBlank(defaultKeyspace) || table.indexOf('.') >= 0 ? table
                : defaultKeyspace + "." + table;
    }
}
