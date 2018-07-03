package com.github.ddth.dao;

import java.util.stream.Stream;

import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoResult;

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
    public <T> DaoResult create(Class<T> clazz, T bo) throws DaoException;

    /**
     * Delete an existing BO from storage.
     * 
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    public <T> DaoResult delete(Class<T> clazz, T bo) throws DaoException;

    /**
     * Fetch an existing BO from storage by id.
     * 
     * @param clazz
     * @param id
     * @return
     * @throws DaoException
     */
    public <T> T get(Class<T> clazz, BoId id) throws DaoException;

    /**
     * Fetch list of existing BOs from storage by id.
     * 
     * @param idList
     * @return
     * @throws DaoException
     */
    public <T> T[] get(Class<T> clazz, BoId... idList) throws DaoException;

    /**
     * Fetch all existing BOs from storage and return the result as a stream.
     * 
     * @param clazz
     * @return
     * @throws DaoException
     */
    public <T> Stream<T> getAll(Class<T> clazz) throws DaoException;

    /**
     * Fetch all existing BOs from storage, sorted by primary key(s) and return the result as a
     * stream.
     * 
     * @param clazz
     * @return
     * @throws DaoException
     */
    public <T> Stream<T> getAllSorted(Class<T> clazz) throws DaoException;

    /**
     * Update an existing BO.
     * 
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    public <T> DaoResult update(Class<T> clazz, T bo) throws DaoException;

    /**
     * Create a new BO or update an existing one.
     * 
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    public <T> DaoResult createOrUpdate(Class<T> clazz, T bo) throws DaoException;

    /**
     * Update an existing BO or create a new one.
     * 
     * @param clazz
     * @param bo
     * @return
     * @throws DaoException
     */
    public <T> DaoResult updateOrCreate(Class<T> clazz, T bo) throws DaoException;
}
