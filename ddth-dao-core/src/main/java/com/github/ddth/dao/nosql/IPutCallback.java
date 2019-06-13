package com.github.ddth.dao.nosql;

/**
 * Callback for PUT operation.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public interface IPutCallback<T> {
    /**
     * Called when PUT operation failed.
     * 
     * @param spaceId
     * @param key
     * @param entry
     * @param t
     */
    void onError(String spaceId, String key, T entry, Throwable t);

    /**
     * Called when PUT operation was successful.
     * 
     * @param spaceId
     * @param key
     * @param entry
     */
    void onSuccess(String spaceId, String key, T entry);
}
