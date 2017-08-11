package com.github.ddth.dao.jdbc;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.github.ddth.dao.BoId;
import com.github.ddth.dao.IGenericBoDao;
import com.github.ddth.dao.utils.CacheInvalidationReason;
import com.github.ddth.dao.utils.DaoExceptionUtils;
import com.github.ddth.dao.utils.DaoResult;
import com.github.ddth.dao.utils.DaoResult.DaoOperationStatus;
import com.github.ddth.dao.utils.DuplicatedKeyException;
import com.github.ddth.dao.utils.DuplicatedUniqueException;

/**
 * Abstract implementation of {@link IGenericBoDao}
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 *
 * @since 0.8.0
 */
public class AbstractGenericBoJdbcDao<T> extends BaseJdbcDao implements IGenericBoDao<T> {

    private String tableName, cacheName;
    private AbstractGenericRowMapper<T> rowMapper;

    private Class<T> typeClass;

    @SuppressWarnings("unchecked")
    public AbstractGenericBoJdbcDao() {
        Class<?> clazz = getClass();
        Type type = clazz.getGenericSuperclass();
        while (type != null) {
            if (type instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                this.typeClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
                break;
            } else {
                clazz = clazz.getSuperclass();
                type = clazz != null ? clazz.getGenericSuperclass() : null;
            }
        }
    }

    public AbstractGenericRowMapper<T> getRowMapper() {
        return rowMapper;
    }

    public AbstractGenericBoJdbcDao<T> setRowMapper(AbstractGenericRowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public AbstractGenericBoJdbcDao<T> setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getCacheName() {
        return cacheName;
    }

    public AbstractGenericBoJdbcDao<T> setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    public AbstractGenericBoJdbcDao<T> init() {
        super.init();

        String[] allCols = rowMapper.getAllColumns();
        String[] pkCols = rowMapper.getPrimaryKeyColumns();
        String[] updateCols = rowMapper.getUpdateColumns();

        List<String> WHERE_PK_INDEX = new ArrayList<>();
        for (String pkCol : pkCols) {
            WHERE_PK_INDEX.add(pkCol + "=?");
        }
        List<String> UPDATE_INDEX = new ArrayList<>();
        for (String updateCol : updateCols) {
            UPDATE_INDEX.add(updateCol + "=?");
        }

        SQL_INSERT = "INSERT INTO {0} (" + StringUtils.join(allCols, ",") + ")VALUES("
                + StringUtils.repeat("?", ",", allCols.length) + ")";

        SQL_DELETE_ONE = "DELETE FROM {0} WHERE " + StringUtils.join(WHERE_PK_INDEX, " AND ");
        SQL_SELECT_ONE = "SELECT " + StringUtils.join(allCols, ",") + " FROM {0} WHERE "
                + StringUtils.join(WHERE_PK_INDEX, " AND ");
        SQL_UPDATE_ONE = "UPDATE {0} SET " + StringUtils.join(UPDATE_INDEX, ",") + " WHERE "
                + StringUtils.join(WHERE_PK_INDEX, " AND ");

        return this;
    }

    private String SQL_SELECT_ONE, SQL_INSERT, SQL_DELETE_ONE, SQL_UPDATE_ONE;

    /**
     * For data partitioning: Sub-class can override this method to calculate name of DB table to
     * access the BO specified by supplied id.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns {@link #getTableName()}.
     * </p>
     * 
     * @param id
     * @return
     * @since 0.8.0.2
     */
    protected String calcTableName(BoId id) {
        return getTableName();
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate name of DB table to
     * access the BO specified by supplied bo.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns {@link #getTableName()}.
     * </p>
     * 
     * @param bo
     * @return
     * @since 0.8.0.2
     */
    protected String calcTableName(T bo) {
        return getTableName();
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * insert the BO by supplied id to DB table.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(BoId)}.
     * </p>
     * 
     * @param id
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlInsert(BoId id) {
        return MessageFormat.format(SQL_INSERT, calcTableName(id));
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * insert the BO by supplied bo to DB table.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(Object)}.
     * </p>
     * 
     * @param bo
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlInsert(T bo) {
        return MessageFormat.format(SQL_INSERT, calcTableName(bo));
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * delete the BO by supplied id.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(BoId)}.
     * </p>
     * 
     * @param id
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlDeleteOne(BoId id) {
        return MessageFormat.format(SQL_DELETE_ONE, calcTableName(id));
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * delete the BO by supplied bo.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(Object)}.
     * </p>
     * 
     * @param bo
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlDeleteOne(T bo) {
        return MessageFormat.format(SQL_DELETE_ONE, calcTableName(bo));
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * select the BO by supplied id.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(BoId)}.
     * </p>
     * 
     * @param id
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlSelectOne(BoId id) {
        return MessageFormat.format(SQL_SELECT_ONE, calcTableName(id));
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * select the BO by supplied bo.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(Object)}.
     * </p>
     * 
     * @param bo
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlSelectOne(T bo) {
        return MessageFormat.format(SQL_SELECT_ONE, calcTableName(bo));
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * update the BO by supplied id.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(BoId)}.
     * </p>
     * 
     * @param id
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlUpdateOne(BoId id) {
        return MessageFormat.format(SQL_UPDATE_ONE, calcTableName(id));
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * update the BO by supplied bo.
     * 
     * <p>
     * This method of class {@link AbstractGenericBoJdbcDao} simple returns its own pre-calculated
     * SQL query with table name substituted by value returned from {@link #calcTableName(Object)}.
     * </p>
     * 
     * @param bo
     * @return
     * @since 0.8.0.2
     */
    protected String calcSqlUpdateOne(T bo) {
        return MessageFormat.format(SQL_UPDATE_ONE, calcTableName(bo));
    }

    /**
     * Calculate cache key for a BO.
     * 
     * @param id
     * @return
     */
    protected String cacheKey(BoId id) {
        return StringUtils.join(id.values, "-");
    }

    /**
     * Calculate cache key for a BO.
     * 
     * @param bo
     * @return
     */
    protected String cacheKey(T bo) {
        return StringUtils.join(rowMapper.valuesForColumns(bo, rowMapper.getPrimaryKeyColumns()),
                "-");
    }

    /**
     * Invalidate a BO from cache.
     * 
     * @param bo
     * @param reason
     */
    protected void invalidateCache(T bo, CacheInvalidationReason reason) {
        final String cacheKey = cacheKey(bo);
        switch (reason) {
        case CREATE:
        case UPDATE:
            putToCache(getCacheName(), cacheKey, bo);
        case DELETE:
            removeFromCache(getCacheName(), cacheKey);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public DaoResult create(T bo) {
        try {
            int numRows = execute(calcSqlInsert(bo),
                    rowMapper.valuesForColumns(bo, rowMapper.getInsertColumns()));
            DaoResult result = numRows > 0 ? new DaoResult(DaoOperationStatus.SUCCESSFUL, bo)
                    : new DaoResult(DaoOperationStatus.ERROR);
            if (numRows > 0) {
                invalidateCache(bo, CacheInvalidationReason.CREATE);
            }
            return result;
        } catch (DuplicatedKeyException dke) {
            return new DaoResult(DaoOperationStatus.DUPLICATED_KEY);
        } catch (DuplicatedUniqueException due) {
            return new DaoResult(DaoOperationStatus.DUPLICATED_UNIQUE);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DaoResult delete(T bo) {
        if (bo == null) {
            return new DaoResult(DaoOperationStatus.NOT_FOUND);
        }
        try {
            int numRows = execute(calcSqlDeleteOne(bo),
                    rowMapper.valuesForColumns(bo, rowMapper.getPrimaryKeyColumns()));
            DaoResult result = numRows > 0 ? new DaoResult(DaoOperationStatus.SUCCESSFUL, bo)
                    : new DaoResult(DaoOperationStatus.NOT_FOUND);
            if (numRows > 0) {
                invalidateCache(bo, CacheInvalidationReason.DELETE);
            }
            return result;
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(BoId id) {
        final String cacheKey = cacheKey(id);
        T bo = getFromCache(getCacheName(), cacheKey, typeClass);
        if (bo == null) {
            try {
                bo = executeSelectOne(rowMapper, calcSqlSelectOne(id), id.values);
            } catch (SQLException e) {
                throw DaoExceptionUtils.translate(e);
            }
            putToCache(getCacheName(), cacheKey, bo);
        }
        return (bo);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T[] get(BoId... idList) {
        List<T> result = new ArrayList<>();
        for (BoId id : idList) {
            T bo = get(id);
            if (bo != null) {
                result.add(bo);
            }
        }
        T[] boList = (T[]) Array.newInstance(typeClass, 0);
        return result.toArray(boList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DaoResult update(T bo) {
        try {
            String[] bindColumns = ArrayUtils.addAll(rowMapper.getUpdateColumns(),
                    rowMapper.getPrimaryKeyColumns());
            int numRows = execute(calcSqlUpdateOne(bo),
                    rowMapper.valuesForColumns(bo, bindColumns));
            DaoResult result = numRows > 0 ? new DaoResult(DaoOperationStatus.SUCCESSFUL, bo)
                    : new DaoResult(DaoOperationStatus.NOT_FOUND);
            if (numRows > 0) {
                invalidateCache(bo, CacheInvalidationReason.UPDATE);
            }
            return result;
        } catch (DuplicatedKeyException dke) {
            return new DaoResult(DaoOperationStatus.DUPLICATED_KEY);
        } catch (DuplicatedUniqueException due) {
            return new DaoResult(DaoOperationStatus.DUPLICATED_UNIQUE);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

}
