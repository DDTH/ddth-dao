package com.github.ddth.dao.nosql.cassandra;

import java.util.Map;

import com.datastax.driver.core.Session;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.cql.SessionManager;
import com.github.ddth.dao.nosql.AbstractNosqlEngine;

/**
 * Base class for Cassandra-specific NoSQL engines.
 * 
 * <p>
 * This engine utilizes Datastax's CQL driver (http://www.datastax.com/) to
 * access Cassandra.
 * </p>
 * 
 * <p>
 * This base Cassandra engine assumes sub-class will implement two method
 * {@link #load(String, String)} and {@link #store(String, String, byte[])}.
 * Hence, default implementation of {@link #loadAsMap(String, String)} delegates
 * the task to {@link #loadAsJson(String, String)}, which in turn delegates
 * method calls to {@link #load(String, String)}. Likewise, default
 * implementation of {@link #store(String, String, Map)} delegates the task to
 * {@link #store(String, String, String)}, which in turn delegates method calls
 * to {@link #store(String, String, byte[])}.
 * </p>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public abstract class BaseCassandraNosqlEngine extends AbstractNosqlEngine {
    private SessionManager sessionManager;
    private String hostsAndPorts;
    private String keyspace, username, password;

    protected SessionManager getSessionManager() {
        return sessionManager;
    }

    public BaseCassandraNosqlEngine setSessionManager(SessionManager sessionManager) {
        this.sessionManager = sessionManager;
        return this;
    }

    protected String getHostsAndPorts() {
        return hostsAndPorts;
    }

    public BaseCassandraNosqlEngine setHostsAndPorts(String hostsAndPorts) {
        this.hostsAndPorts = hostsAndPorts;
        return this;
    }

    protected String getKeyspace() {
        return keyspace;
    }

    public BaseCassandraNosqlEngine setKeyspace(String keyspace) {
        this.keyspace = keyspace;
        return this;
    }

    protected String getUsername() {
        return username;
    }

    public BaseCassandraNosqlEngine setUsername(String username) {
        this.username = username;
        return this;
    }

    protected String getPassword() {
        return password;
    }

    public BaseCassandraNosqlEngine setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * Obtains a Cassandra Session.
     * 
     * @return
     */
    protected Session getSession() {
        return getSession(keyspace);
    }

    /**
     * Obtains a Cassandra Session.
     * 
     * @param keyspace
     * @return
     */
    protected Session getSession(String keyspace) {
        return getSession(hostsAndPorts, username, username, keyspace);
    }

    /**
     * Obtains a Cassandra Session.
     * 
     * @param hostsAndPorts
     * @param username
     * @param password
     * @param keyspace
     * @return
     */
    protected Session getSession(String hostsAndPorts, String username, String password,
            String keyspace) {
        return sessionManager.getSession(hostsAndPorts, username, password, keyspace);
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     * 
     * <p>
     * This method simply returns {@code null}. Sub-class overrides this method
     * to implement its own business.
     * </p>
     */
    @Override
    public byte[] load(String storageId, String entryId) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String loadAsJson(String storageId, String entryId) {
        byte[] data = load(storageId, entryId);
        return data != null ? new String(data, CHARSET) : "null";
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<Object, Object> loadAsMap(String storageId, String entryId) {
        String json = loadAsJson(storageId, entryId);
        return SerializationUtils.fromJsonString(json, Map.class);
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * This method is a no-op. Sub-class overrides this method to implement its
     * own business logic.
     * </p>
     */
    @Override
    public void delete(String tableName, String entryId) {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     * 
     * <p>
     * This method is a no-op. Sub-class overrides this method to implement its
     * own business logic.
     * </p>
     */
    @Override
    public void store(String storageId, String entryId, byte[] data) {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(String storageId, String entryId, String jsonData) {
        byte[] data = jsonData != null ? jsonData.getBytes(CHARSET) : null;
        store(storageId, entryId, data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void store(String storageId, String entryId, Map<Object, Object> data) {
        String json = SerializationUtils.toJsonString(data);
        store(storageId, entryId, json);
    }
}
