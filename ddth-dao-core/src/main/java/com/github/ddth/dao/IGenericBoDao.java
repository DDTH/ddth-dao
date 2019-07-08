package com.github.ddth.dao;

import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoResult;

import java.util.stream.Stream;

/**
 * API interface for DAO that manages one single BO class.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public interface IGenericBoDao<T> {
    /**
     * Create/Persist a new BO to storage.
     *
     * @param bo
     * @return
     * @throws DaoException
     */
    DaoResult create(T bo) throws DaoException;

    /**
     * Delete an existing BO from storage.
     *
     * @param bo
     * @return
     * @throws DaoException
     */
    DaoResult delete(T bo) throws DaoException;

    /**
     * Fetch an existing BO from storage by id.
     *
     * @param id
     * @return
     * @throws DaoException
     */
    T get(BoId id) throws DaoException;

    /**
     * Fetch list of existing BOs from storage by id.
     *
     * @param idList
     * @return
     * @throws DaoException
     */
    T[] get(BoId... idList) throws DaoException;

    /**
     * Fetch all existing BOs from storage and return the result as a stream.
     *
     * @return
     * @throws DaoException
     * @since 0.9.0
     */
    Stream<T> getAll() throws DaoException;

    /**
     * Fetch all existing BOs from storage, sorted by primary key(s) and return the result as a
     * stream.
     *
     * @return
     * @throws DaoException
     * @since 0.9.0
     */
    Stream<T> getAllSorted() throws DaoException;

    /**
     * Update an existing BO.
     *
     * @param bo
     * @return
     * @throws DaoException
     */
    DaoResult update(T bo) throws DaoException;

    /**
     * Create a new BO or update an existing one.
     *
     * @param bo
     * @return
     * @throws DaoException
     * @since 0.8.1
     */
    @SuppressWarnings("deprecation")
    default DaoResult createOrUpdate(T bo) throws DaoException {
        if (bo == null) {
            return null;
        }
        DaoResult result = create(bo);
        DaoResult.DaoOperationStatus status = result != null ? result.getStatus() : null;
        if (status == DaoResult.DaoOperationStatus.DUPLICATED_VALUE
                || status == DaoResult.DaoOperationStatus.DUPLICATED_UNIQUE) {
            result = update(bo);
        }
        return result;
    }

    /**
     * Update an existing BO or create a new one.
     *
     * @param bo
     * @return
     * @throws DaoException
     * @since 0.8.1
     */
    default DaoResult updateOrCreate(T bo) throws DaoException {
        if (bo == null) {
            return new DaoResult(DaoResult.DaoOperationStatus.NOT_FOUND);
        }
        DaoResult result = update(bo);
        DaoResult.DaoOperationStatus status = result != null ? result.getStatus() : null;
        if (status == DaoResult.DaoOperationStatus.NOT_FOUND) {
            result = create(bo);
        }
        return result;
    }
}
