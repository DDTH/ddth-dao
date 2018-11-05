package com.github.ddth.dao.nosql;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for {key:value} NoSQL-based DAOs.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class BaseKvDao extends BaseNoSqlDao implements IKvStorage {
    private final Logger LOGGER = LoggerFactory.getLogger(BaseKvDao.class);

    private IKvStorage kvStorage;

    /**
     * Getter for {@link #kvStorage}.
     * 
     * @return
     */
    public IKvStorage getKvStorage() {
        return kvStorage;
    }

    /**
     * Setter for {@link #kvStorage}.
     * 
     * @param kvStorage
     * @return
     */
    public BaseKvDao setKvStorage(IKvStorage kvStorage) {
        this.kvStorage = kvStorage;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String spaceId, String key) throws IOException {
        kvStorage.delete(spaceId, key, new IDeleteCallback() {
            @Override
            public void onSuccess(String spaceId, String key) {
                // invalidate cache upon successful deletion
                invalidateCacheEntry(spaceId, key);
            }

            @Override
            public void onError(String spaceId, String key, Throwable t) {
                LOGGER.error(t.getMessage(), t);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String spaceId, String key, IDeleteCallback callback) throws IOException {
        kvStorage.delete(spaceId, key, new IDeleteCallback() {
            @Override
            public void onSuccess(String spaceId, String key) {
                // invalidate cache upon successful deletion
                invalidateCacheEntry(spaceId, key);
                // invoke callback
                callback.onSuccess(spaceId, key);
            }

            @Override
            public void onError(String spaceId, String key, Throwable t) {
                // invoke callback
                callback.onError(spaceId, key, t);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean keyExists(String spaceId, String key) throws IOException {
        /*
         * Call get(spaceId, key)!=null or delegate to kvStorage.keyExists(spaceId, key)?
         * Since DELETE and PUT operations can be async, delegating to kvStorage.keyExists(spaceId,
         * key) would be preferred to avoid out-of-date data.
         */
        return kvStorage.keyExists(spaceId, key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] get(String spaceId, String key) throws IOException {
        String cacheKey = calcCacheKey(spaceId, key);
        byte[] data = getFromCache(getCacheName(), cacheKey, byte[].class);
        if (data == null) {
            data = kvStorage.get(spaceId, key);
            putToCache(getCacheName(), cacheKey, data);
        }
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(IKvEntryMapper<T> mapper, String spaceId, String key) throws IOException {
        byte[] data = get(spaceId, key);
        return data != null ? mapper.mapEntry(spaceId, key, data) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String spaceId, String key, byte[] value) throws IOException {
        kvStorage.put(spaceId, key, value, new IPutCallback<byte[]>() {
            @Override
            public void onSuccess(String spaceId, String key, byte[] entry) {
                // invalidate cache upon successful deletion
                invalidateCacheEntry(spaceId, key, entry);
            }

            @Override
            public void onError(String spaceId, String key, byte[] entry, Throwable t) {
                LOGGER.error(t.getMessage(), t);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String spaceId, String key, byte[] value, IPutCallback<byte[]> callback)
            throws IOException {
        kvStorage.put(spaceId, key, value, new IPutCallback<byte[]>() {
            @Override
            public void onSuccess(String spaceId, String key, byte[] entry) {
                // invalidate cache upon successful deletion
                invalidateCacheEntry(spaceId, key, entry);
                // invoke callback
                callback.onSuccess(spaceId, key, entry);
            }

            @Override
            public void onError(String spaceId, String key, byte[] entry, Throwable t) {
                // invoke callback
                callback.onError(spaceId, key, entry, t);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size(String spaceId) throws IOException {
        return kvStorage.size(spaceId);
    }
}
