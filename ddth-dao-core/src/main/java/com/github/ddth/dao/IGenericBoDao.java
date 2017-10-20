package com.github.ddth.dao;

import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoResult;

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
    public DaoResult create(T bo) throws DaoException;

    /**
     * Delete an existing BO from storage.
     * 
     * @param bo
     * @return
     * @throws DaoException
     */
    public DaoResult delete(T bo) throws DaoException;

    /**
     * Fetch an existing BO from storage by id.
     * 
     * @param id
     * @return
     * @throws DaoException
     */
    public T get(BoId id) throws DaoException;

    /**
     * Fetch list of existing BOs from storage by id.
     * 
     * @param idList
     * @return
     * @throws DaoException
     */
    public T[] get(BoId... idList) throws DaoException;

    /**
     * Update an existing BO.
     * 
     * @param bo
     * @return
     * @throws DaoException
     */
    public DaoResult update(T bo) throws DaoException;

    /**
     * Create a new BO or update an existing one.
     * 
     * @param bo
     * @return
     * @since 0.8.1
     * @throws DaoException
     */
    public DaoResult createOrUpdate(T bo) throws DaoException;

    /**
     * Update an existing BO or create a new one.
     * 
     * @param bo
     * @return
     * @since 0.8.1
     * @throws DaoException
     */
    public DaoResult updateOrCreate(T bo) throws DaoException;
}
