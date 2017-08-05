package com.github.ddth.dao.jdbc.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.dao.BaseDao;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DaoExceptionUtils;

/**
 * This implementation of {@link IJdbcHelper} utilizes Spring's
 * {@link JdbcTemplate} to interact with database.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class JdbcTemplateJdbcHelper extends AbstractJdbcHelper {

    /**
     * Get {@link JdbcTemplate} instance for a given {@link Connection}.
     * 
     * Note: the returned {@link JdbcTemplate} will not automatically close the
     * {@link Connection}.
     * 
     * @param conn
     * @return
     */
    protected JdbcTemplate jdbcTemplate(Connection conn) {
        DataSource ds = new SingleConnectionDataSource(conn, true);
        return new JdbcTemplate(ds);
    }

    /**
     * Get {@link NamedParameterJdbcTemplate} instance for a given
     * {@link Connection}.
     * 
     * Note: the returned {@link JdbcTemplate} will not automatically close the
     * {@link Connection}.
     * 
     * @param conn
     * @return
     * @since 0.8.0
     */
    protected NamedParameterJdbcTemplate namedParameterJdbcTemplate(Connection conn) {
        DataSource ds = new SingleConnectionDataSource(conn, true);
        return new NamedParameterJdbcTemplate(ds);
    }

    /**
     * Get a {@link JdbcTemplate} instance.
     * 
     * @return
     */
    protected JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(getDataSource());
    }

    /**
     * Get a {@link NamedParameterJdbcTemplate} instance.
     * 
     * @return
     * @since 0.8.0
     */
    protected NamedParameterJdbcTemplate namedParameterJdbcTemplate() {
        return new NamedParameterJdbcTemplate(getDataSource());
    }

    /*--------------------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Object... bindValues) {
        try (Connection conn = getConnection()) {
            return execute(jdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Map<String, ?> bindValues) {
        try (Connection conn = getConnection()) {
            return execute(namedParameterJdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Object... bindValues) {
        return execute(jdbcTemplate(conn), sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Map<String, ?> bindValues) {
        return execute(namedParameterJdbcTemplate(conn), sql, bindValues);
    }

    /**
     * Executes a non-SELECT statement.
     * 
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return number of affected rows
     */
    private int execute(JdbcTemplate jdbcTemplate, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            return bindValues.length > 0 ? jdbcTemplate.update(sql, bindValues)
                    : jdbcTemplate.update(sql);
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * Executes a non-SELECT statement.
     * 
     * @param namedParameterJdbcTemplate
     * @param sql
     * @param bindValues
     *            named-based bind value
     * @return number of affected rows
     */
    private int execute(NamedParameterJdbcTemplate namedParameterJdbcTemplate, String sql,
            Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            return namedParameterJdbcTemplate.update(sql, bindValues);
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelect(rowMapper, jdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql,
            Map<String, ?> bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelect(rowMapper, namedParameterJdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) {
        return executeSelect(rowMapper, jdbcTemplate(conn), sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelect(rowMapper, namedParameterJdbcTemplate(conn), sql, bindValues);
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param rowMapper
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     */
    private <T> List<T> executeSelect(IRowMapper<T> rowMapper, JdbcTemplate jdbcTemplate,
            String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rowMapper.mapRow(rs, rowNum);
                }
            };
            return bindValues.length > 0 ? jdbcTemplate.query(sql, jRowMapper, bindValues)
                    : jdbcTemplate.query(sql, jRowMapper);
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param rowMapper
     * @param namedParameterJdbcTemplate
     * @param sql
     * @param bindValues
     *            named-based bind values
     * @return
     */
    private <T> List<T> executeSelect(IRowMapper<T> rowMapper,
            NamedParameterJdbcTemplate namedParameterJdbcTemplate, String sql,
            Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rowMapper.mapRow(rs, rowNum);
                }
            };
            return namedParameterJdbcTemplate.query(sql, bindValues, jRowMapper);
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Object... bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelect(jdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Map<String, ?> bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelect(namedParameterJdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Object... bindValues) {
        return executeSelect(jdbcTemplate(conn), sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelect(namedParameterJdbcTemplate(conn), sql, bindValues);
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     */
    private List<Map<String, Object>> executeSelect(JdbcTemplate jdbcTemplate, String sql,
            Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            return bindValues.length > 0 ? jdbcTemplate.queryForList(sql, bindValues)
                    : jdbcTemplate.queryForList(sql);
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param namedParameterJdbcTemplate
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     */
    private List<Map<String, Object>> executeSelect(
            NamedParameterJdbcTemplate namedParameterJdbcTemplate, String sql,
            Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            return namedParameterJdbcTemplate.queryForList(sql, bindValues);
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Object... bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelectOne(rowMapper, jdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelectOne(rowMapper, namedParameterJdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) {
        return executeSelectOne(rowMapper, jdbcTemplate(conn), sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelectOne(rowMapper, namedParameterJdbcTemplate(conn), sql, bindValues);
    }

    /**
     * Executes a SELECT statement and fetch one row.
     * 
     * @param rowMapper
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     */
    private <T> T executeSelectOne(IRowMapper<T> rowMapper, JdbcTemplate jdbcTemplate, String sql,
            Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rowMapper.mapRow(rs, rowNum);
                }
            };
            jdbcTemplate.setFetchSize(1);
            List<T> rows = bindValues.length > 0 ? jdbcTemplate.query(sql, jRowMapper, bindValues)
                    : jdbcTemplate.query(sql, jRowMapper);
            return rows != null && rows.size() > 0 ? rows.get(0) : null;
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * Executes a SELECT statement and fetch one row.
     * 
     * @param rowMapper
     * @param namedParameterJdbcTemplate
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     */
    private <T> T executeSelectOne(IRowMapper<T> rowMapper,
            NamedParameterJdbcTemplate namedParameterJdbcTemplate, String sql,
            Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rowMapper.mapRow(rs, rowNum);
                }
            };
            List<T> rows = namedParameterJdbcTemplate.query(sql, bindValues, jRowMapper);
            return rows != null && rows.size() > 0 ? rows.get(0) : null;
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Object... bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelectOne(jdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Map<String, ?> bindValues) {
        try (Connection conn = getConnection()) {
            return executeSelectOne(namedParameterJdbcTemplate(conn), sql, bindValues);
        } catch (SQLException e) {
            throw new DaoException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql, Object... bindValues) {
        return executeSelectOne(jdbcTemplate(conn), sql, bindValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql,
            Map<String, ?> bindValues) {
        return executeSelectOne(namedParameterJdbcTemplate(conn), sql, bindValues);
    }

    /**
     * Executes a SELECT statement and fetch one row.
     * 
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     *            index-based bind values
     * @return
     * @since 0.8.0
     */
    private Map<String, Object> executeSelectOne(JdbcTemplate jdbcTemplate, String sql,
            Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            jdbcTemplate.setFetchSize(1);
            List<Map<String, Object>> rows = bindValues.length > 0
                    ? jdbcTemplate.queryForList(sql, bindValues) : jdbcTemplate.queryForList(sql);
            return rows != null && rows.size() > 0 ? rows.get(0) : null;
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * Executes a SELECT statement and fetch one row.
     * 
     * @param namedParameterJdbcTemplate
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.0
     */
    private Map<String, Object> executeSelectOne(
            NamedParameterJdbcTemplate namedParameterJdbcTemplate, String sql,
            Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            List<Map<String, Object>> rows = namedParameterJdbcTemplate.queryForList(sql,
                    bindValues);
            return rows != null && rows.size() > 0 ? rows.get(0) : null;
        } catch (DataAccessException e) {
            throw DaoExceptionUtils.translate(e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }
}
