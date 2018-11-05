package com.github.ddth.dao.nosql;

import com.github.ddth.dao.BaseDao;

/**
 * Base class for NoSQL-based DAOs.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class BaseNoSqlDao extends BaseDao {
    private String cacheName;

    /**
     * Name of the cache this DAO uses to cache data.
     * 
     * @return
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Name of the cache this DAO uses to cache data.
     * 
     * @param cacheName
     * @return
     */
    public BaseNoSqlDao setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    /**
     * Calculate cache key for an entry.
     * 
     * @param spaceId
     * @param key
     * @return
     */
    protected String calcCacheKey(String spaceId, String key) {
        return spaceId + ":" + key;
    }

    /**
     * Invalidate a cache entry.
     * 
     * @param spaceId
     * @param key
     */
    protected void invalidateCacheEntry(String spaceId, String key) {
        if (isCacheEnabled()) {
            removeFromCache(getCacheName(), calcCacheKey(spaceId, key));
        }
    }

    /**
     * Invalidate a cache entry due to updated content.
     * 
     * @param spaceId
     * @param key
     * @param data
     */
    protected void invalidateCacheEntry(String spaceId, String key, Object data) {
        if (isCacheEnabled()) {
            putToCache(getCacheName(), calcCacheKey(spaceId, key), data);
        }
    }
}
