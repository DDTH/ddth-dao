package com.github.ddth.dao;

/**
 * API interface for DAO that manage one single BO class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public interface IBoDao<T extends BaseBo> {
    /**
     * Create/Persist a new BO to storage.
     * 
     * @param bo
     * @return
     */
    public DaoResult create(T bo);

    /**
     * Delete an existing BO from storage.
     * 
     * @param bo
     * @return
     */
    public DaoResult delete(T bo);

    /**
     * Fetch an existing BO from storage
     * 
     * @param id
     * @return
     */
    public T get(Object id);

    /**
     * Update an existing BO.
     * 
     * @param bo
     * @return
     */
    public DaoResult update(T bo);
}
