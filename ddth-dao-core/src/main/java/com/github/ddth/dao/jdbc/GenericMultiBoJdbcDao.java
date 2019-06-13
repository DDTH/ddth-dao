package com.github.ddth.dao.jdbc;

import com.github.ddth.dao.BoId;
import com.github.ddth.dao.IGenericBoDao;
import com.github.ddth.dao.IGenericMultiBoDao;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoResult;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

/**
 * Generic implementation of {@link IGenericMultiBoDao}
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.9.0
 */
public class GenericMultiBoJdbcDao extends BaseJdbcDao implements IGenericMultiBoDao {

    private ConcurrentMap<Class<?>, IGenericBoDao<?>> delegateDaos = new ConcurrentHashMap<>();

    /**
     * Thrown when the delegate DAO is not found.
     */
    public static class DelegateDaoNotFound extends DaoException {
        private static final long serialVersionUID = "0.9.0".hashCode();

        public DelegateDaoNotFound() {
        }

        public DelegateDaoNotFound(String message) {
            super(message);
        }

        public DelegateDaoNotFound(Throwable cause) {
            super(cause);
        }

        public DelegateDaoNotFound(String message, Throwable cause) {
            super(message, cause);
        }

        public DelegateDaoNotFound(String message, Throwable cause, boolean enableSuppression,
                boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    /**
     * Lookup the delegate dao.
     *
     * @param clazz
     * @return
     */
    @SuppressWarnings("unchecked")
    protected <T> IGenericBoDao<T> lookupDelegateDao(Class<T> clazz) throws DelegateDaoNotFound {
        IGenericBoDao<?> result = delegateDaos.get(clazz);
        if (result == null) {
            throw new DelegateDaoNotFound("Delegate dao for [" + clazz + "] not found.");
        }
        return (IGenericBoDao<T>) result;
    }

    /**
     * Add a delegate dao to mapping list.
     *
     * @param clazz
     * @param dao
     * @return
     */
    public <T> GenericMultiBoJdbcDao addDelegateDao(Class<T> clazz, IGenericBoDao<T> dao) {
        delegateDaos.put(clazz, dao);
        return this;
    }

    /**
     * Add a delegate dao to mapping list.
     *
     * @param className
     * @param dao
     * @return
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public <T> GenericMultiBoJdbcDao addDelegateDao(String className, IGenericBoDao<T> dao)
            throws ClassNotFoundException {
        Class<T> clazz = (Class<T>) Class.forName(className);
        return addDelegateDao(clazz, dao);
    }

    /**
     * Set delegate dao mappings.
     *
     * @param daoMappings
     * @return
     */
    public GenericMultiBoJdbcDao setDelegateDaos(Map<?, IGenericBoDao<?>> daoMappings) throws ClassNotFoundException {
        delegateDaos.clear();
        for (Entry<?, IGenericBoDao<?>> entry : daoMappings.entrySet()) {
            Object cl = entry.getKey();
            IGenericBoDao<?> dao = entry.getValue();
            if (cl instanceof Class) {
                delegateDaos.put((Class<?>) cl, dao);
            } else {
                addDelegateDao(cl.toString(), dao);
            }
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> DaoResult create(Class<T> clazz, T bo) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.create(bo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> DaoResult delete(Class<T> clazz, T bo) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.delete(bo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T get(Class<T> clazz, BoId id) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.get(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T[] get(Class<T> clazz, BoId... idList) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.get(idList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> getAll(Class<T> clazz) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.getAll();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Stream<T> getAllSorted(Class<T> clazz) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.getAllSorted();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> DaoResult update(Class<T> clazz, T bo) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.update(bo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> DaoResult createOrUpdate(Class<T> clazz, T bo) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.createOrUpdate(bo);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> DaoResult updateOrCreate(Class<T> clazz, T bo) throws DaoException {
        IGenericBoDao<T> dao = lookupDelegateDao(clazz);
        return dao.updateOrCreate(bo);
    }
}
