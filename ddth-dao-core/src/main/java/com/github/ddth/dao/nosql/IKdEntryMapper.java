package com.github.ddth.dao.nosql;

import java.util.Map;

/**
 * An interface to map {@code key-document} entry to object.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public interface IKdEntryMapper<T> {
    /**
     * Map a {@code key-document} entry to object.
     * 
     * @param spaceId
     * @param key
     * @param document
     * @return
     */
    T mapEntry(String spaceId, String key, Map<String, Object> document);
}
