package com.github.ddth.dao.nosql;

import java.util.Collection;
import java.util.Map;

/**
 * APIs to access the underlying NoSQL storage.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public interface INosqlEngine {
    /**
     * Deletes an existing entry from storage.
     * 
     * @param storageId
     * @param entryId
     */
    public void delete(String storageId, String entryId);

    /**
     * Gets list of available entry-ids.
     * 
     * @param storageId
     * @return
     * @since 0.3.1
     */
    public Collection<String> entryIdList(String storageId);

    /**
     * Loads an entry from storage.
     * 
     * @param storageId
     * @param entryId
     * @return
     */
    public byte[] load(String storageId, String entryId);

    /**
     * Loads an entry from storage as a JSON string.
     * 
     * @param storageId
     * @param entryId
     * @return
     */
    public String loadAsJson(String storageId, String entryId);

    /**
     * Loads an entry from storage as a Map.
     * 
     * @param storageId
     * @param entryId
     * @return
     */
    public Map<Object, Object> loadAsMap(String storageId, String entryId);

    /**
     * Stores an entry to storage.
     * 
     * @param storageId
     * @param entryId
     * @param data
     */
    public void store(String storageId, String entryId, byte[] data);

    /**
     * Stores an entry to storage.
     * 
     * @param storageId
     * @param entryId
     * @param jsonData
     */
    public void store(String storageId, String entryId, String jsonData);

    /**
     * Stores an entry to storage.
     * 
     * @param storageId
     * @param entryId
     * @param data
     */
    public void store(String storageId, String entryId, Map<Object, Object> data);
}
