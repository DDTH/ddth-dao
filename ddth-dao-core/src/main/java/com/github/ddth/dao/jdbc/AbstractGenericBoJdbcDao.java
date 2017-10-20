package com.github.ddth.dao.jdbc;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
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

    /**
     * @return
     * @since 0.8.0.4
     */
    protected Class<T> getTypeClass() {
        return typeClass;
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
        String[] insCols = rowMapper.getInsertColumns();
        String[] updateCols = rowMapper.getUpdateColumns();
        String[] pkCols = rowMapper.getPrimaryKeyColumns();
        String checksumCol = rowMapper.getChecksumColumn();

        List<String> WHERE_PK_INDEX = new ArrayList<>();
        List<String> WHERE_PK_AND_CHECKSUM_INDEX = new ArrayList<>();
        for (String pkCol : pkCols) {
            WHERE_PK_INDEX.add(pkCol + "=?");
        }
        WHERE_PK_AND_CHECKSUM_INDEX.addAll(WHERE_PK_INDEX);
        if (!StringUtils.isBlank(checksumCol)) {
            WHERE_PK_AND_CHECKSUM_INDEX.add(checksumCol + "!=?");
        }
        List<String> UPDATE_INDEX = new ArrayList<>();
        for (String updateCol : updateCols) {
            UPDATE_INDEX.add(updateCol + "=?");
        }

        SQL_INSERT = "INSERT INTO {0} (" + StringUtils.join(insCols, ",") + ")VALUES("
                + StringUtils.repeat("?", ",", insCols.length) + ")";

        SQL_DELETE_ONE = "DELETE FROM {0} WHERE " + StringUtils.join(WHERE_PK_INDEX, " AND ");
        SQL_SELECT_ONE = "SELECT " + StringUtils.join(allCols, ",") + " FROM {0} WHERE "
                + StringUtils.join(WHERE_PK_INDEX, " AND ");
        SQL_UPDATE_ONE = "UPDATE {0} SET " + StringUtils.join(UPDATE_INDEX, ",") + " WHERE "
                + StringUtils.join(WHERE_PK_AND_CHECKSUM_INDEX, " AND ");

        // System.out.println(SQL_SELECT_ONE);
        // System.out.println(SQL_INSERT);
        // System.out.println(SQL_DELETE_ONE);
        // System.out.println(SQL_UPDATE_ONE);

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
     * Create/Persist a new BO to storage.
     * 
     * @param conn
     * @param bo
     * @return
     * @since 0.8.1
     */
    protected DaoResult create(Connection conn, T bo) {
        try {
            int numRows = execute(conn, calcSqlInsert(bo),
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
    public DaoResult create(T bo) {
        try (Connection conn = getConnection()) {
            return create(conn, bo);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

    /**
     * Delete an existing BO from storage.
     * 
     * @param conn
     * @param bo
     * @return
     * @since 0.8.1
     */
    protected DaoResult delete(Connection conn, T bo) {
        if (bo == null) {
            return new DaoResult(DaoOperationStatus.NOT_FOUND);
        }
        try {
            int numRows = execute(conn, calcSqlDeleteOne(bo),
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
    public DaoResult delete(T bo) {
        if (bo == null) {
            return new DaoResult(DaoOperationStatus.NOT_FOUND);
        }
        try (Connection conn = getConnection()) {
            return delete(conn, bo);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

    /**
     * Fetch an existing BO from storage by id.
     * 
     * @param conn
     * @param id
     * @return
     */
    protected T get(Connection conn, BoId id) {
        final String cacheKey = cacheKey(id);
        T bo = getFromCache(getCacheName(), cacheKey, typeClass);
        if (bo == null) {
            try {
                bo = executeSelectOne(rowMapper, conn, calcSqlSelectOne(id), id.values);
            } catch (SQLException e) {
                throw DaoExceptionUtils.translate(e);
            }
            putToCache(getCacheName(), cacheKey, bo);
        }
        return bo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(BoId id) {
        final String cacheKey = cacheKey(id);
        T bo = getFromCache(getCacheName(), cacheKey, typeClass);
        if (bo == null) {
            try (Connection conn = getConnection()) {
                return get(conn, id);
            } catch (SQLException e) {
                throw DaoExceptionUtils.translate(e);
            }
        }
        return bo;
    }

    /**
     * Fetch list of existing BOs from storage by id.
     * 
     * @param conn
     * @param idList
     * @return
     * @since 0.8.1
     */
    @SuppressWarnings("unchecked")
    protected T[] get(Connection conn, BoId... idList) {
        List<T> result = new ArrayList<>();
        for (BoId id : idList) {
            T bo = get(conn, id);
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
    public T[] get(BoId... idList) {
        try (Connection conn = getConnection()) {
            return get(conn, idList);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

    /**
     * Update an existing BO.
     * 
     * @param conn
     * @param bo
     * @return
     * @since 0.8.1
     */
    protected DaoResult update(Connection conn, T bo) {
        try {
            String[] bindColumns = ArrayUtils.addAll(rowMapper.getUpdateColumns(),
                    rowMapper.getPrimaryKeyColumns());
            String colChecksum = rowMapper.getChecksumColumn();
            if (!StringUtils.isBlank(colChecksum)) {
                bindColumns = ArrayUtils.add(bindColumns, colChecksum);
            }
            int numRows = execute(conn, calcSqlUpdateOne(bo),
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

    /**
     * {@inheritDoc}
     */
    @Override
    public DaoResult update(T bo) {
        try (Connection conn = getConnection()) {
            return update(conn, bo);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

    /**
     * Create a new BO or update an existing one.
     * 
     * @param conn
     * @param bo
     * @return
     * @since 0.8.1
     */
    protected DaoResult createOrUpdate(Connection conn, T bo) {
        DaoResult result = create(conn, bo);
        DaoOperationStatus status = result.getStatus();
        if (status == DaoOperationStatus.DUPLICATED_KEY
                || status == DaoOperationStatus.DUPLICATED_UNIQUE) {
            result = update(conn, bo);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.8.1
     */
    @Override
    public DaoResult createOrUpdate(T bo) {
        try (Connection conn = getConnection()) {
            return createOrUpdate(conn, bo);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }

    /**
     * Update an existing BO or create a new one.
     * 
     * @param conn
     * @param bo
     * @return
     * @since 0.8.1
     */
    protected DaoResult updateOrCreate(Connection conn, T bo) {
        DaoResult result = update(conn, bo);
        DaoOperationStatus status = result.getStatus();
        if (status == DaoOperationStatus.NOT_FOUND) {
            result = create(conn, bo);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.8.1
     */
    @Override
    public DaoResult updateOrCreate(T bo) {
        try (Connection conn = getConnection()) {
            return updateOrCreate(conn, bo);
        } catch (SQLException e) {
            throw DaoExceptionUtils.translate(e);
        }
    }
}
