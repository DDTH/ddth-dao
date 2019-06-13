package com.github.ddth.dao.nosql;

/**
 * APIs to map {@code key-value} entry to object.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public interface IKvEntryMapper<T> {
    /**
     * Map a {@code key-value} entry to object.
     * 
     * @param spaceId
     * @param key
     * @param value
     * @return
     */
    T mapEntry(String spaceId, String key, byte[] value);
}
