package com.github.ddth.dao;

import com.github.ddth.cacheadapter.CacheException;
import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.cacheadapter.ICacheFactory;
import com.github.ddth.dao.utils.ProfilingRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * Base class for application DAOs.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class BaseDao implements AutoCloseable {

    private final Logger LOGGER = LoggerFactory.getLogger(BaseDao.class);

    private static ThreadLocal<List<ProfilingRecord>> profilingRecords = ThreadLocal
            .withInitial(() -> new LinkedList<>());

    /**
     * Initializes profiling data.
     */
    public static void startProfiling() {
        profilingRecords.remove();
    }

    /**
     * Clears profiling data.
     */
    public static void clearProfiling() {
        profilingRecords.remove();
    }

    /**
     * Gets current profiling data.
     *
     * @return
     */
    public static ProfilingRecord[] getProfiling() {
        return profilingRecords.get().toArray(ProfilingRecord.EMPTY_ARRAY);
    }

    /**
     * Adds a new profiling record.
     *
     * @param execTimeMs
     * @param command
     * @return
     */
    public static ProfilingRecord addProfiling(long execTimeMs, String command, long durationMs) {
        ProfilingRecord record = new ProfilingRecord(execTimeMs, command, durationMs);
        List<ProfilingRecord> records = profilingRecords.get();
        records.add(record);
        while (records.size() > 100) {
            records.remove(0);
        }
        return record;
    }

    /*--------------------------------------------------------------------------------*/

    private ICacheFactory cacheFactory;
    private boolean cacheItemsExpireAfterWrite = false;

    /**
     * Initializing method.
     *
     * @return
     */
    public BaseDao init() {
        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.8.5
     */
    @Override
    public void close() {
        this.destroy();
    }

    /**
     * Cache factory used to cache DAO data.
     *
     * @return
     */
    protected ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

    /**
     * Cache factory used to cache DAO data.
     *
     * @param cacheFactory
     * @return
     */
    public BaseDao setCacheFactory(ICacheFactory cacheFactory) {
        this.cacheFactory = cacheFactory;
        return this;
    }

    protected boolean isCacheEnabled() {
        return cacheFactory != null;
    }

    protected ICache getCache(String cacheName) {
        return cacheFactory != null ? cacheFactory.createCache(cacheName) : null;
    }

    /**
     * Check if cache items, by default, expire after write or read/access.
     *
     * @return
     */
    public boolean isCacheItemsExpireAfterWrite() {
        return cacheItemsExpireAfterWrite;
    }

    /**
     * Set if cache items, by default, expire after write or read/access.
     *
     * @param cacheItemsExpireAfterWrite
     */
    public void setCacheItemsExpireAfterWrite(boolean cacheItemsExpireAfterWrite) {
        this.cacheItemsExpireAfterWrite = cacheItemsExpireAfterWrite;
    }

    /**
     * Remove an entry from cache.
     *
     * @param cacheName
     * @param key
     */
    protected void removeFromCache(String cacheName, String key) {
        try {
            ICache cache = getCache(cacheName);
            if (cache != null) {
                cache.delete(key);
            }
        } catch (CacheException e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * Put an entry to cache, with default TTL.
     *
     * @param cacheName
     * @param key
     * @param value
     */
    protected void putToCache(String cacheName, String key, Object value) {
        putToCache(cacheName, key, value, 0);
    }

    /**
     * Put an entry to cache, with specific TTL.
     *
     * @param cacheName
     * @param key
     * @param value
     * @param ttlSeconds
     */
    protected void putToCache(String cacheName, String key, Object value, long ttlSeconds) {
        if (cacheItemsExpireAfterWrite) {
            putToCache(cacheName, key, value, ttlSeconds, 0);
        } else {
            putToCache(cacheName, key, value, 0, ttlSeconds);
        }
    }

    /**
     * Put an entry to cache, with specific {@code expireAfterWriteSeconds/expireAfterAccessSeconds}.
     *
     * @param cacheName
     * @param key
     * @param value
     * @param expireAfterWriteSeconds
     * @param expireAfterAccessSeconds
     */
    protected void putToCache(String cacheName, String key, Object value, long expireAfterWriteSeconds,
            long expireAfterAccessSeconds) {
        try {
            ICache cache = getCache(cacheName);
            if (value != null && cache != null) {
                cache.set(key, value, expireAfterWriteSeconds, expireAfterAccessSeconds);
            }
        } catch (CacheException e) {
            LOGGER.warn(e.getMessage(), e);
        }
    }

    /**
     * Get an entry from cache.
     *
     * @param cacheName
     * @param key
     * @return
     */
    protected Object getFromCache(String cacheName, String key) {
        try {
            ICache cache = getCache(cacheName);
            return cache != null && key != null ? cache.get(key) : null;
        } catch (CacheException e) {
            LOGGER.warn(e.getMessage(), e);
            return null;
        }
    }

    /**
     * Get an entry from cache.
     *
     * <p>
     * Note: if the object from cache is not assignable to clazz,
     * <code>null</code> is returned.
     * </p>
     *
     * @param cacheName
     * @param key
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    protected <T> T getFromCache(String cacheName, String key, Class<T> clazz) {
        Object obj = getFromCache(cacheName, key);
        if (obj == null) {
            return null;
        }
        if (clazz.isAssignableFrom(obj.getClass())) {
            return (T) obj;
        }
        return null;
    }
}
