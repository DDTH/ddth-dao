package com.github.ddth.dao.nosql;

import java.io.IOException;

/**
 * APIs to access the {key:value} NoSQL storage.
 * 
 * <ul>
 * <li>Storage can be divided into spaces, each space is identified by an unique id</li>
 * <li>{@code key: String}</li>
 * <li>{@code value: byte[]}</li>
 * </ul>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public interface IKvStorage {
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
    byte[] get(String spaceId, String key) throws IOException;

    /**
     * Get/Load an entry by key.
     * 
     * @param mapper
     * @param spaceId
     * @param key
     * @return
     * @throws IOException
     */
    default <T> T get(IKvEntryMapper<T> mapper, String spaceId, String key) throws IOException {
        byte[] value = get(spaceId, key);
        return value != null ? mapper.mapEntry(spaceId, key, value) : null;
    }

    /**
     * Put/Store an entry.
     * 
     * @param spaceId
     * @param key
     * @param value
     * @throws IOException
     */
    default void put(String spaceId, String key, byte[] value) throws IOException {
        put(spaceId, key, value, null);
    }

    /**
     * Put/Store an entry.
     * 
     * @param spaceId
     * @param key
     * @param value
     * @param callback
     * @throws IOException
     */
    void put(String spaceId, String key, byte[] value, IPutCallback<byte[]> callback)
            throws IOException;

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
