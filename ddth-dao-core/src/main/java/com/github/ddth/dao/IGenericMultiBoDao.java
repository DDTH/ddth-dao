package com.github.ddth.dao;

import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoResult;

import java.util.stream.Stream;

/**
 * API interface for DAO that manages multi-BO classes.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.9.0
 */
public interface IGenericMultiBoDao {
    /**
     * Create/Persist a new BO to storage.
     *
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    <T> DaoResult create(Class<T> clazz, T bo) throws DaoException;

    /**
     * Delete an existing BO from storage.
     *
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    <T> DaoResult delete(Class<T> clazz, T bo) throws DaoException;

    /**
     * Fetch an existing BO from storage by id.
     *
     * @param clazz
     * @param id
     * @return
     * @throws DaoException
     */
    <T> T get(Class<T> clazz, BoId id) throws DaoException;

    /**
     * Fetch list of existing BOs from storage by id.
     *
     * @param idList
     * @return
     * @throws DaoException
     */
    <T> T[] get(Class<T> clazz, BoId... idList) throws DaoException;

    /**
     * Fetch all existing BOs from storage and return the result as a stream.
     *
     * @param clazz
     * @return
     * @throws DaoException
     */
    <T> Stream<T> getAll(Class<T> clazz) throws DaoException;

    /**
     * Fetch all existing BOs from storage, sorted by primary key(s) and return the result as a
     * stream.
     *
     * @param clazz
     * @return
     * @throws DaoException
     */
    <T> Stream<T> getAllSorted(Class<T> clazz) throws DaoException;

    /**
     * Update an existing BO.
     *
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    <T> DaoResult update(Class<T> clazz, T bo) throws DaoException;

    /**
     * Create a new BO or update an existing one.
     *
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    @SuppressWarnings("deprecation")
    default <T> DaoResult createOrUpdate(Class<T> clazz, T bo) throws DaoException {
        if (clazz == null || bo == null) {
            return null;
        }
        DaoResult result = create(clazz, bo);
        DaoResult.DaoOperationStatus status = result != null ? result.getStatus() : null;
        if (status == DaoResult.DaoOperationStatus.DUPLICATED_VALUE
                || status == DaoResult.DaoOperationStatus.DUPLICATED_UNIQUE) {
            result = update(clazz, bo);
        }
        return result;
    }

    /**
     * Update an existing BO or create a new one.
     *
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    default <T> DaoResult updateOrCreate(Class<T> clazz, T bo) throws DaoException {
        if (clazz == null || bo == null) {
            return new DaoResult(DaoResult.DaoOperationStatus.NOT_FOUND);
        }
        DaoResult result = update(clazz, bo);
        DaoResult.DaoOperationStatus status = result != null ? result.getStatus() : null;
        if (status == DaoResult.DaoOperationStatus.NOT_FOUND) {
            result = create(clazz, bo);
        }
        return result;
    }
}
