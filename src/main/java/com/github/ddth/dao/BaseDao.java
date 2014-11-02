package com.github.ddth.dao;

import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;

import com.github.ddth.cacheadapter.ICache;
import com.github.ddth.cacheadapter.ICacheFactory;

/**
 * Base class for application DAOs.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class BaseDao {

    protected final static Charset CHARSET = Charset.forName("UTF-8");

    public final static String DEFAULT_DATASOURCE_NAME = "default";

    private static ThreadLocal<List<ProfilingRecord>> profilingRecords = new ThreadLocal<List<ProfilingRecord>>() {
        @Override
        protected List<ProfilingRecord> initialValue() {
            return new LinkedList<ProfilingRecord>();
        }
    };

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
    public static ProfilingRecord addProfiling(long execTimeMs, String command) {
        ProfilingRecord record = new ProfilingRecord(execTimeMs, command);
        profilingRecords.get().add(record);
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
        // EMPTY
    }

    protected ICacheFactory getCacheFactory() {
        return cacheFactory;
    }

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
     * Checks if cache items, by default, expire after write or read/access.
     * 
     * @return
     */
    public boolean isCacheItemsExpireAfterWrite() {
        return cacheItemsExpireAfterWrite;
    }

    /**
     * Sets if cache items, by default, expire after write or read/access.
     * 
     * @param cacheItemsExpireAfterWrite
     */
    public void setCacheItemsExpireAfterWrite(boolean cacheItemsExpireAfterWrite) {
        this.cacheItemsExpireAfterWrite = cacheItemsExpireAfterWrite;
    }

    /**
     * Removes an entry from cache.
     * 
     * @param cacheName
     * @param key
     */
    protected void removeFromCache(String cacheName, String key) {
        ICache cache = getCache(cacheName);
        if (cache != null) {
            cache.delete(key);
        }
    }

    /**
     * Puts an entry to cache, with default TTL.
     * 
     * @param cacheName
     * @param key
     * @param value
     */
    protected void putToCache(String cacheName, String key, Object value) {
        putToCache(cacheName, key, value, 0);
    }

    /**
     * Puts an entry to cache, with specific TTL.
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
     * Puts an entry to cache, with specific expireAfterWrite/expireAfterRead.
     * 
     * @param cacheName
     * @param key
     * @param value
     * @param expireAfterWriteSeconds
     * @param expireAfterAccessSeconds
     */
    protected void putToCache(String cacheName, String key, Object value,
            long expireAfterWriteSeconds, long expireAfterAccessSeconds) {
        ICache cache = getCache(cacheName);
        if (value != null && cache != null) {
            cache.set(key, value, expireAfterWriteSeconds, expireAfterAccessSeconds);
        }
    }

    /**
     * Gets an entry from cache.
     * 
     * @param cacheName
     * @param key
     * @return
     */
    protected Object getFromCache(String cacheName, String key) {
        ICache cache = getCache(cacheName);
        return cache != null ? cache.get(key) : null;
    }

    /**
     * Gets an entry from cache.
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
