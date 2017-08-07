package com.github.ddth.dao.jdbc;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.sql.SQLException;
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

    private final Class<T> typeClass;

    @SuppressWarnings("unchecked")
    public AbstractGenericBoJdbcDao() {
        ParameterizedType parameterizedType = (ParameterizedType) getClass().getGenericSuperclass();
        this.typeClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
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

        SQL_SELECT_ONE = "SELECT " + StringUtils.join(allCols, ",") + " FROM " + tableName
                + " WHERE " + StringUtils.join(WHERE_PK_INDEX, " AND ");
        SQL_INSERT = "INSERT INTO " + tableName + "(" + StringUtils.join(allCols, ",") + ")VALUES("
                + StringUtils.repeat("?", ",", allCols.length) + ")";
        SQL_DELETE = "DELETE FROM " + tableName + " WHERE "
                + StringUtils.join(WHERE_PK_INDEX, " AND ");
        SQL_UPDATE_ONE = "UPDATE " + tableName + " SET " + StringUtils.join(UPDATE_INDEX, ",")
                + " WHERE " + StringUtils.join(WHERE_PK_INDEX, " AND ");

        return this;
    }

    private String SQL_SELECT_ONE, SQL_INSERT, SQL_DELETE, SQL_UPDATE_ONE;

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
     * Invalidate a BO from cache.
     * 
     * @param bo
     * @param reason
     */
    protected void invalidateCache(T bo, CacheInvalidationReason reason) {
        // TODO
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public DaoResult create(T bo) {
        try {
            int numRows = execute(SQL_INSERT,
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
            int numRows = execute(SQL_DELETE,
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
        T bo = getFromCache(cacheName, cacheKey, typeClass);
        if (bo == null) {
            try {
                bo = executeSelectOne(rowMapper, SQL_SELECT_ONE, id.values);
            } catch (SQLException e) {
                throw DaoExceptionUtils.translate(e);
            }
            putToCache(cacheName, cacheKey, bo);
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
            int numRows = execute(SQL_UPDATE_ONE, rowMapper.valuesForColumns(bo, bindColumns));
            DaoResult result = numRows > 0 ? new DaoResult(DaoOperationStatus.SUCCESSFUL, bo)
                    : new DaoResult(DaoOperationStatus.NOT_FOUND);
            if (numRows > 0) {
                invalidateCache(bo, CacheInvalidationReason.DELETE);
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
