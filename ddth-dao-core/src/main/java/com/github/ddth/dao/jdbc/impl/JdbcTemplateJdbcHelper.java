package com.github.ddth.dao.jdbc.impl;

import com.github.ddth.dao.BaseDao;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.utils.JdbcHelper;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

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
     * <p>
     * Note: the returned {@link JdbcTemplate} will not close the wrapped {@link Connection}!
     * </p>
     *
     * @param conn
     * @return
     */
    protected JdbcTemplate jdbcTemplate(Connection conn) {
        return new JdbcTemplate(new SingleConnectionDataSource(conn, true));
    }

    /**
     * Get {@link NamedParameterJdbcTemplate} instance for a given
     * {@link Connection}.
     *
     * <p>
     * Note: the returned {@link JdbcTemplate} will not close the wrapped {@link Connection}!
     * </p>
     *
     * @param conn
     * @return
     * @since 0.8.0
     */
    protected NamedParameterJdbcTemplate namedParameterJdbcTemplate(Connection conn) {
        return new NamedParameterJdbcTemplate(new SingleConnectionDataSource(conn, true));
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
    public int execute(Connection conn, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            return bindValues != null && bindValues.length > 0 ?
                    jdbcTemplate.update(sql, bindValues) :
                    jdbcTemplate.update(sql);
        } catch (DataAccessException dae) {
            throw translateSQLException(dae);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatementCreator psc = con -> JdbcHelper.prepareAndBindNamedParamsStatement(conn, sql, bindValues);
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            return jdbcTemplate.update(psc);
        } catch (DataAccessException dae) {
            throw translateSQLException(dae);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = (rs, rowNum) -> rowMapper.mapRow(rs, rowNum);
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            int fetchSize = getDefaultFetchSize();
            jdbcTemplate.setFetchSize(fetchSize < 0 ? Integer.MIN_VALUE : fetchSize);
            return bindValues != null && bindValues.length > 0 ?
                    jdbcTemplate.query(sql, jRowMapper, bindValues) :
                    jdbcTemplate.query(sql, jRowMapper);
        } catch (DataAccessException dae) {
            throw translateSQLException(dae);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql, Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = (rs, rowNum) -> rowMapper.mapRow(rs, rowNum);
            PreparedStatementCreator psc = con -> JdbcHelper.prepareAndBindNamedParamsStatement(conn, sql, bindValues);
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            int fetchSize = getDefaultFetchSize();
            jdbcTemplate.setFetchSize(fetchSize < 0 ? Integer.MIN_VALUE : fetchSize);
            return jdbcTemplate.query(psc, jRowMapper);
        } catch (DataAccessException dae) {
            throw translateSQLException(dae);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }
}
