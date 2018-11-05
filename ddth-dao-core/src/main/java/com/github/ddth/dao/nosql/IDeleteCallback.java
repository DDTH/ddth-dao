package com.github.ddth.dao.nosql;

/**
 * Callback for DELETE operation.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public interface IDeleteCallback {
    /**
     * Called when DELETE operation failed.
     * 
     * @param spaceId
     * @param key
     * @param t
     */
    void onError(String spaceId, String key, Throwable t);

    /**
     * Called when DELETE operation successful.
     * 
     * @param spaceId
     * @param key
     */
    void onSuccess(String spaceId, String key);
}
