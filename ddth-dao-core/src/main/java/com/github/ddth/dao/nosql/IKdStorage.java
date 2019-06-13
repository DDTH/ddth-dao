package com.github.ddth.dao.nosql;

import java.io.IOException;
import java.util.Map;

/**
 * APIs to access the {key:document} NoSQL storage.
 * 
 * <ul>
 * <li>Storage can be divided into spaces (namespaces), each space is identified by an unique id</li>
 * <li>{@code key: String}</li>
 * <li>{@code document: Map<String,?>}</li>
 * </ul>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public interface IKdStorage {
    /**
     * Delete an existing entry from storage.
     * 
     * @param spaceId
     * @param key
     * @throws IOException
     */
    default void delete(String spaceId, String key) throws IOException {
        delete(spaceId, key, null);
    }

    /**
     * Delete an existing entry from storage.
     * 
     * @param spaceId
     * @param key
     * @param callback
     * @throws IOException
     */
    void delete(String spaceId, String key, IDeleteCallback callback) throws IOException;

    /**
     * Check if a key exists.
     * 
     * @param spaceId
     * @param key
     * @return
     * @throws IOException
     */
    default boolean keyExists(String spaceId, String key) throws IOException {
        return get(spaceId, key) != null;
    }

    /**
     * Get/Load an entry by key.
     * 
     * @param spaceId
     * @param key
     * @return
     * @throws IOException
     */
    Map<String, Object> get(String spaceId, String key) throws IOException;

    /**
     * Get/Load an entry by key.
     * 
     * @param mapper
     * @param spaceId
     * @param key
     * @return
     * @throws IOException
     */
    default <T> T get(IKdEntryMapper<T> mapper, String spaceId, String key) throws IOException {
        Map<String, Object> doc = get(spaceId, key);
        return doc != null ? mapper.mapEntry(spaceId, key, doc) : null;
    }

    /**
     * Put/Store an entry.
     * 
     * @param spaceId
     * @param key
     * @param document
     * @throws IOException
     */
    default void put(String spaceId, String key, Map<String, Object> document) throws IOException {
        put(spaceId, key, document, null);
    }

    /**
     * Put/Store an entry.
     * 
     * @param spaceId
     * @param key
     * @param document
     * @param callback
     * @throws IOException
     */
    void put(String spaceId, String key, Map<String, Object> document,
            IPutCallback<Map<String, Object>> callback) throws IOException;

    /**
     * Return number of entries.
     * 
     * @param spaceId
     * @return number of entries currently in the storage, {@code -1} if counting number entries is
     *         not supported
     * @throws IOException
     */
    long size(String spaceId) throws IOException;
}
