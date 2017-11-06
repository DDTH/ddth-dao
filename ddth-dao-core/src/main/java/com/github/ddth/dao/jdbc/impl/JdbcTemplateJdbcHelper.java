package com.github.ddth.dao.jdbc.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.dao.BaseDao;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.utils.JdbcHelper;

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
    public int execute(Connection conn, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            return bindValues != null && bindValues.length > 0
                    ? jdbcTemplate.update(sql, bindValues) : jdbcTemplate.update(sql);
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
            PreparedStatementCreator psc = new PreparedStatementCreator() {
                @Override
                public PreparedStatement createPreparedStatement(Connection con)
                        throws SQLException {
                    return JdbcHelper.prepareAndBindNamedParamsStatement(conn, sql, bindValues);
                }
            };
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
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rowMapper.mapRow(rs, rowNum);
                }
            };
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            int fetchSize = getDefaultFetchSize();
            jdbcTemplate.setFetchSize(fetchSize < 0 ? Integer.MIN_VALUE : fetchSize);
            return bindValues != null && bindValues.length > 0
                    ? jdbcTemplate.query(sql, jRowMapper, bindValues)
                    : jdbcTemplate.query(sql, jRowMapper);
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
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            RowMapper<T> jRowMapper = new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rowMapper.mapRow(rs, rowNum);
                }
            };
            PreparedStatementCreator psc = new PreparedStatementCreator() {
                @Override
                public PreparedStatement createPreparedStatement(Connection con)
                        throws SQLException {
                    return JdbcHelper.prepareAndBindNamedParamsStatement(conn, sql, bindValues);
                }
            };
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

    /*----------------------------------------------------------------------*/
    // /**
    // * {@inheritDoc}
    // */
    // @Override
    // public <T> Stream<T> executeSelectAsStream(IRowMapper<T> rowMapper, Connection conn,
    // int fetchSize, String sql, Object... bindValues) throws DaoException {
    //
    // }
}
