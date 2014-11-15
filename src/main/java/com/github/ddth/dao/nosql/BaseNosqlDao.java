package com.github.ddth.dao.nosql;

import java.util.Collection;
import java.util.Map;

import com.github.ddth.dao.BaseDao;

/**
 * Base class for NoSQL-based DAOs.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class BaseNosqlDao extends BaseDao {
    private INosqlEngine nosqlEngine;

    protected INosqlEngine getNosqlEngine() {
        return nosqlEngine;
    }

    public BaseNosqlDao setNosqlEngine(INosqlEngine nosqlEngine) {
        this.nosqlEngine = nosqlEngine;
        return this;
    }

    /**
     * Deletes an entry from storage.
     * 
     * @param storageId
     * @param entryId
     */
    protected void delete(String storageId, String entryId) {
        nosqlEngine.delete(storageId, entryId);
    }

    /**
     * Gets list of available entry-ids.
     * 
     * @param storageId
     * @return
     * @since 0.3.1
     */
    protected Collection<String> entryIdList(String storageId) {
        return nosqlEngine.entryIdList(storageId);
    }

    /**
     * Loads an entry from storage.
     * 
     * @param storageId
     * @param entryId
     * @return
     */
    protected byte[] load(String storageId, String entryId) {
        return nosqlEngine.load(storageId, entryId);
    }

    /**
     * Loads an entry from storage as a JSON string.
     * 
     * @param storageId
     * @param entryId
     * @return
     */
    protected String loadAsJson(String storageId, String entryId) {
        return nosqlEngine.loadAsJson(storageId, entryId);
    }

    /**
     * Loads an entry from storage as a Map.
     * 
     * @param storageId
     * @param entryId
     * @return
     */
    protected Map<Object, Object> loadAsMap(String storageId, String entryId) {
        return nosqlEngine.loadAsMap(storageId, entryId);
    }

    /**
     * Stores an entry to storage.
     * 
     * @param storageId
     * @param entryId
     * @param data
     */
    protected void store(String storageId, String entryId, byte[] data) {
        nosqlEngine.store(storageId, entryId, data);
    }

    /**
     * Stores an entry to storage.
     * 
     * @param storageId
     * @param entryId
     * @param jsonData
     */
    protected void store(String storageId, String entryId, String jsonData) {
        nosqlEngine.store(storageId, entryId, jsonData);
    }

    /**
     * Stores an entry to storage.
     * 
     * @param storageId
     * @param entryId
     * @param data
     */
    protected void store(String storageId, String entryId, Map<Object, Object> data) {
        nosqlEngine.store(storageId, entryId, data);
    }
}
