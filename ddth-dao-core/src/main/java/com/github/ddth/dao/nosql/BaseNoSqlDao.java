package com.github.ddth.dao.nosql;

import com.github.ddth.dao.BaseDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for NoSQL-based DAOs.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class BaseNoSqlDao extends BaseDao {
    private final Logger LOGGER = LoggerFactory.getLogger(BaseNoSqlDao.class);

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

    /**
     * @param callback
     * @return
     * @since 1.0.0
     */
    protected IDeleteCallback wrapCallback(IDeleteCallback callback) {
        return new IDeleteCallback() {
            @Override
            public void onSuccess(String spaceId, String key) {
                // invalidate cache upon successful deletion
                invalidateCacheEntry(spaceId, key);
                if (callback != null) {
                    callback.onSuccess(spaceId, key);
                }
            }

            @Override
            public void onError(String spaceId, String key, Throwable t) {
                if (callback != null) {
                    callback.onError(spaceId, key, t);
                } else {
                    LOGGER.error(t.getMessage(), t);
                }
            }
        };
    }
}
