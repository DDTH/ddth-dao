package com.github.ddth.dao.jdbc;

import com.github.ddth.dao.BoId;
import com.github.ddth.dao.IGenericBoDao;
import com.github.ddth.dao.utils.CacheInvalidationReason;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoResult;
import com.github.ddth.dao.utils.DaoResult.DaoOperationStatus;
import com.github.ddth.dao.utils.DuplicatedValueException;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Generic implementation of {@link IGenericBoDao}
 *
 * <p>
 * Note: this class must be *abstract* in order to correctly detect the generic typed parameter!
 * </p>
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public abstract class GenericBoJdbcDao<T> extends BaseJdbcDao implements IGenericBoDao<T> {

    private boolean upsertInTransaction = true;

    private String tableName, cacheName;
    private AbstractGenericRowMapper<T> rowMapper;
    private String cacheKeyPrefix = "";

    private Class<T> typeClass;

    @SuppressWarnings("unchecked")
    public GenericBoJdbcDao() {
        Class<?> clazz = getClass();
        Type type = clazz.getGenericSuperclass();
        while (type != null) {
            if (type instanceof ParameterizedType) {
                ParameterizedType parameterizedType = (ParameterizedType) type;
                Type type1 = parameterizedType.getActualTypeArguments()[0];
                if (type1 instanceof ParameterizedType) {
                    // for the case MyDao extends AbstractGenericBoJdbcDao<AGeneticClass<T>>
                    this.typeClass = (Class<T>) ((ParameterizedType) type1).getRawType();
                } else {
                    // for the case MyDao extends AbstractGenericBoJdbcDao<AClass>
                    this.typeClass = (Class<T>) type1;
                }
                break;
            } else {
                // current class does not have parameter(s), but its super might
                // e.g. MyChildDao extends MyDao extends AbstractGenericBoJdbcDao<T>
                clazz = clazz.getSuperclass();
                type = clazz != null ? clazz.getGenericSuperclass() : null;
            }
        }
    }

    /**
     * Should "upsert" ({@link #createOrUpdate(Object)} and {@link #updateOrCreate(Object)} be done
     * in a transaction context?
     *
     * @return {@code true} if "upsert" should be done in a transaction context, {@code false}
     * otherwise
     * @since 0.9.0.5
     */
    public boolean isUpsertInTransaction() {
        return upsertInTransaction;
    }

    /**
     * Should "upsert" ({@link #createOrUpdate(Object)} and {@link #updateOrCreate(Object)} be done
     * in a transaction context?
     *
     * @param upsertInTransaction {@code true} if "upsert" should be done in a transaction context, {@code false}
     *                            otherwise
     * @since 0.9.0.5
     */
    public void setUpsertInTransaction(boolean upsertInTransaction) {
        this.upsertInTransaction = upsertInTransaction;
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

    public GenericBoJdbcDao<T> setRowMapper(AbstractGenericRowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public GenericBoJdbcDao<T> setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getCacheName() {
        return cacheName;
    }

    public GenericBoJdbcDao<T> setCacheName(String cacheName) {
        this.cacheName = cacheName;
        return this;
    }

    public GenericBoJdbcDao<T> init() {
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

        SQL_SELECT_ALL = "SELECT " + StringUtils.join(allCols, ",") + " FROM {0}";
        SQL_SELECT_ALL_SORTED = pkCols != null && pkCols.length > 0 ?
                "SELECT " + StringUtils.join(allCols, ",") + " FROM {0} ORDER BY " + StringUtils.join(pkCols, ",") :
                null;
        SQL_SELECT_ONE = pkCols != null && pkCols.length > 0 ?
                "SELECT " + StringUtils.join(allCols, ",") + " FROM {0} WHERE " + StringUtils
                        .join(WHERE_PK_INDEX, " AND ") :
                null;
        SQL_INSERT = "INSERT INTO {0} (" + StringUtils.join(insCols, ",") + ") VALUES (" + StringUtils
                .repeat("?", ",", insCols.length) + ")";
        SQL_DELETE_ONE = pkCols != null && pkCols.length > 0 ?
                "DELETE FROM {0} WHERE " + StringUtils.join(WHERE_PK_INDEX, " AND ") :
                null;
        SQL_UPDATE_ONE = updateCols != null && updateCols.length > 0 ?
                "UPDATE {0} SET " + StringUtils.join(UPDATE_INDEX, ",") + " WHERE " + StringUtils
                        .join(WHERE_PK_AND_CHECKSUM_INDEX, " AND ") :
                null;

        return this;
    }

    private String SQL_SELECT_ALL, SQL_SELECT_ALL_SORTED, SQL_SELECT_ONE, SQL_INSERT, SQL_DELETE_ONE, SQL_UPDATE_ONE;

    /**
     * For data partitioning: Sub-class can override this method to calculate name of DB table to
     * access the BO specified by supplied id.
     *
     * <p>
     * This method of class {@link GenericBoJdbcDao} simple returns {@link #getTableName()}.
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
     * This method of class {@link GenericBoJdbcDao} simple returns {@link #getTableName()}.
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
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * Calculate the SQL query to select all rows.
     *
     * @return
     * @since 0.9.0
     */
    protected String calcSqlSelectAll() {
        return MessageFormat.format(SQL_SELECT_ALL, getTableName());
    }

    /**
     * Calculate the SQL query to select all rows.
     *
     * @return
     * @since 0.9.0
     */
    protected String calcSqlSelectAllSorted() {
        return MessageFormat.format(SQL_SELECT_ALL_SORTED, getTableName());
    }

    /**
     * For data partitioning: Sub-class can override this method to calculate the SQL query to
     * update the BO by supplied id.
     *
     * <p>
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * This method of class {@link GenericBoJdbcDao} simple returns its own pre-calculated
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
     * Get string prefixed to cache key.
     *
     * @return
     * @since 0.8.2
     */
    public String getCacheKeyPrefix() {
        return cacheKeyPrefix;
    }

    /**
     * Set string prefixed to cache key.
     *
     * @param cacheKeyPrefix
     * @since 0.8.2
     */
    public void setCacheKeyPrefix(String cacheKeyPrefix) {
        this.cacheKeyPrefix = cacheKeyPrefix;
    }

    /**
     * Calculate cache key for a BO.
     *
     * @param id
     * @return
     */
    protected String cacheKey(BoId id) {
        String result = StringUtils.join(id.values, "-");
        return StringUtils.isBlank(cacheKeyPrefix) ? result : (cacheKeyPrefix + result);
    }

    /**
     * Calculate cache key for a BO.
     *
     * @param bo
     * @return
     */
    protected String cacheKey(T bo) {
        String result = StringUtils.join(rowMapper.valuesForColumns(bo, rowMapper.getPrimaryKeyColumns()), "-");
        return StringUtils.isBlank(cacheKeyPrefix) ? result : (cacheKeyPrefix + result);
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
        if (bo == null) {
            return null;
        }
        Savepoint savepoint = null;
        try {
            try {
                savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
                int numRows = execute(conn, calcSqlInsert(bo),
                        rowMapper.valuesForColumns(bo, rowMapper.getInsertColumns()));
                DaoResult result = numRows > 0 ?
                        new DaoResult(DaoOperationStatus.SUCCESSFUL, bo) :
                        new DaoResult(DaoOperationStatus.ERROR);
                if (numRows > 0) {
                    invalidateCache(bo, CacheInvalidationReason.CREATE);
                }
                return result;
            } catch (DuplicatedValueException dke) {
                if (savepoint != null) {
                    conn.rollback(savepoint);
                }
                return new DaoResult(DaoOperationStatus.DUPLICATED_VALUE);
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DaoResult create(T bo) {
        if (bo == null) {
            return null;
        }
        try (Connection conn = getConnection()) {
            return create(conn, bo);
        } catch (SQLException e) {
            throw new DaoException(e);
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
        int numRows = execute(conn, calcSqlDeleteOne(bo),
                rowMapper.valuesForColumns(bo, rowMapper.getPrimaryKeyColumns()));
        DaoResult result = numRows > 0 ?
                new DaoResult(DaoOperationStatus.SUCCESSFUL, bo) :
                new DaoResult(DaoOperationStatus.NOT_FOUND);
        if (numRows > 0) {
            invalidateCache(bo, CacheInvalidationReason.DELETE);
        }
        return result;
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
            throw new DaoException(e);
        }
    }

    private static class NotFoundException extends Exception {
        private static final long serialVersionUID = "1.0.0".hashCode();
    }

    private T _getFromCache(BoId id) throws NotFoundException {
        if (id == null || id.values == null || id.values.length == 0) {
            throw new NotFoundException();
        }
        String cacheKey = cacheKey(id);
        return getFromCache(getCacheName(), cacheKey, typeClass);
    }

    /**
     * Fetch an existing BO from storage by id.
     *
     * @param conn
     * @param id
     * @return
     */
    protected T get(Connection conn, BoId id) {
        try {
            T bo = _getFromCache(id);
            if (bo == null) {
                bo = executeSelectOne(rowMapper, conn, calcSqlSelectOne(id), id.values);
                putToCache(getCacheName(), cacheKey(id), bo);
            }
            return bo;
        } catch (NotFoundException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(BoId id) {
        try {
            T bo = _getFromCache(id);
            if (bo == null) {
                try (Connection conn = getConnection()) {
                    bo = get(conn, id);
                } catch (SQLException e) {
                    throw new DaoException(e);
                }
            }
            return bo;
        } catch (NotFoundException e) {
            return null;
        }
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
        T[] result = (T[]) Array.newInstance(typeClass, idList != null ? idList.length : 0);
        if (idList != null) {
            for (int i = 0; i < idList.length; i++) {
                result[i] = get(conn, idList[i]);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T[] get(BoId... idList) {
        if (idList == null || idList.length == 0) {
            return (T[]) Array.newInstance(typeClass, 0);
        }
        try (Connection conn = getConnection()) {
            return get(conn, idList);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * Fetch all existing BOs from storage and return the result as a stream.
     *
     * @param conn
     * @return
     * @since 0.9.0
     */
    protected Stream<T> getAll(Connection conn) {
        return executeSelectAsStream(rowMapper, conn, true, calcSqlSelectAll());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<T> getAll() {
        return getAll(getConnection());
    }

    /**
     * Fetch all existing BOs from storage, sorted by primary key(s) and return the result as a
     * stream.
     *
     * @param conn
     * @return
     * @since 0.9.0
     */
    protected Stream<T> getAllSorted(Connection conn) {
        return executeSelectAsStream(rowMapper, conn, true, calcSqlSelectAllSorted());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<T> getAllSorted() {
        return getAllSorted(getConnection());
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
        if (bo == null) {
            return new DaoResult(DaoOperationStatus.NOT_FOUND);
        }
        Savepoint savepoint = null;
        try {
            try {
                String[] bindColumns = ArrayUtils
                        .addAll(rowMapper.getUpdateColumns(), rowMapper.getPrimaryKeyColumns());
                String colChecksum = rowMapper.getChecksumColumn();
                if (!StringUtils.isBlank(colChecksum)) {
                    bindColumns = ArrayUtils.add(bindColumns, colChecksum);
                }
                savepoint = conn.getAutoCommit() ? null : conn.setSavepoint();
                int numRows = execute(conn, calcSqlUpdateOne(bo), rowMapper.valuesForColumns(bo, bindColumns));
                DaoResult result = numRows > 0 ?
                        new DaoResult(DaoOperationStatus.SUCCESSFUL, bo) :
                        new DaoResult(DaoOperationStatus.NOT_FOUND);
                if (numRows > 0) {
                    invalidateCache(bo, CacheInvalidationReason.UPDATE);
                }
                return result;
            } catch (DuplicatedValueException dke) {
                if (savepoint != null) {
                    conn.rollback(savepoint);
                }
                return new DaoResult(DaoOperationStatus.DUPLICATED_VALUE);
            }
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DaoResult update(T bo) {
        if (bo == null) {
            return new DaoResult(DaoOperationStatus.NOT_FOUND);
        }
        try (Connection conn = getConnection()) {
            return update(conn, bo);
        } catch (SQLException e) {
            throw new DaoException(e);
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
    @SuppressWarnings("deprecation")
    protected DaoResult createOrUpdate(Connection conn, T bo) {
        if (bo == null) {
            return null;
        }
        DaoResult result = create(conn, bo);
        DaoResult.DaoOperationStatus status = result != null ? result.getStatus() : null;
        if (status == DaoOperationStatus.DUPLICATED_VALUE || status == DaoOperationStatus.DUPLICATED_UNIQUE) {
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
        if (bo == null) {
            return null;
        }
        try (Connection conn = getConnection(upsertInTransaction)) {
            return createOrUpdate(conn, bo);
        } catch (SQLException e) {
            throw new DaoException(e);
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
        if (bo == null) {
            return new DaoResult(DaoOperationStatus.NOT_FOUND);
        }
        DaoResult result = update(conn, bo);
        DaoResult.DaoOperationStatus status = result != null ? result.getStatus() : null;
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
        if (bo == null) {
            return new DaoResult(DaoOperationStatus.NOT_FOUND);
        }
        try (Connection conn = getConnection(upsertInTransaction)) {
            return updateOrCreate(conn, bo);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }
}
