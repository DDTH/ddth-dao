package com.github.ddth.dao.nosql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Base class for {key:document} NoSQL-based DAOs.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.10.0
 */
public class BaseKdDao extends BaseNoSqlDao implements IKdStorage {
    private final Logger LOGGER = LoggerFactory.getLogger(BaseKdDao.class);

    private IKdStorage kdStorage;

    /**
     * Getter for {@link #kdStorage}.
     *
     * @return
     */
    public IKdStorage getKdStorage() {
        return kdStorage;
    }

    /**
     * Setter for {@link #kdStorage}.
     *
     * @param kdStorage
     * @return
     */
    public BaseKdDao setKdStorage(IKdStorage kdStorage) {
        this.kdStorage = kdStorage;
        return this;
    }

    //    /**
    //     * {@inheritDoc}
    //     */
    //    @Override
    //    public void delete(String spaceId, String key) throws IOException {
    //        kdStorage.delete(spaceId, key, new IDeleteCallback() {
    //            @Override
    //            public void onSuccess(String spaceId, String key) {
    //                // invalidate cache upon successful deletion
    //                invalidateCacheEntry(spaceId, key);
    //            }
    //
    //            @Override
    //            public void onError(String spaceId, String key, Throwable t) {
    //                LOGGER.error(t.getMessage(), t);
    //            }
    //        });
    //    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void delete(String spaceId, String key, IDeleteCallback callback) throws IOException {
        kdStorage.delete(spaceId, key, wrapCallback(callback));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean keyExists(String spaceId, String key) throws IOException {
        /*
         * Call get(spaceId, key)!=null or delegate to kdStorage.keyExists(spaceId, key)?
         *
         * Since DELETE and PUT operations can be async, delegating to kdStorage.keyExists(spaceId,
         * key) would be preferred to avoid out-of-date data.
         */
        return kdStorage.keyExists(spaceId, key);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Map<String, Object> get(String spaceId, String key) throws IOException {
        String cacheKey = calcCacheKey(spaceId, key);
        Map<String, Object> data = getFromCache(getCacheName(), cacheKey, Map.class);
        if (data == null) {
            data = kdStorage.get(spaceId, key);
            putToCache(getCacheName(), cacheKey, data);
        }
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(IKdEntryMapper<T> mapper, String spaceId, String key) throws IOException {
        Map<String, Object> data = get(spaceId, key);
        return data != null ? mapper.mapEntry(spaceId, key, data) : null;
    }

    //    /**
    //     * {@inheritDoc}
    //     */
    //    @Override
    //    public void put(String spaceId, String key, Map<String, Object> document) throws IOException {
    //        kdStorage.put(spaceId, key, document, new IPutCallback<Map<String, Object>>() {
    //            @Override
    //            public void onSuccess(String spaceId, String key, Map<String, Object> entry) {
    //                // invalidate cache upon successful deletion
    //                invalidateCacheEntry(spaceId, key, entry);
    //            }
    //
    //            @Override
    //            public void onError(String spaceId, String key, Map<String, Object> entry, Throwable t) {
    //                LOGGER.error(t.getMessage(), t);
    //            }
    //        });
    //    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(String spaceId, String key, Map<String, Object> document,
            IPutCallback<Map<String, Object>> callback) throws IOException {
        kdStorage.put(spaceId, key, document, new IPutCallback<>() {
            @Override
            public void onSuccess(String spaceId, String key, Map<String, Object> entry) {
                // invalidate cache upon successful deletion
                invalidateCacheEntry(spaceId, key, entry);
                if (callback != null) {
                    callback.onSuccess(spaceId, key, entry);
                }
            }

            @Override
            public void onError(String spaceId, String key, Map<String, Object> entry, Throwable t) {
                if (callback != null) {
                    callback.onError(spaceId, key, entry, t);
                } else {
                    LOGGER.error(t.getMessage(), t);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long size(String spaceId) throws IOException {
        return kdStorage.size(spaceId);
    }
}
