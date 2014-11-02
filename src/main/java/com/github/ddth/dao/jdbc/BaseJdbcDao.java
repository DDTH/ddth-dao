package com.github.ddth.dao.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.github.ddth.dao.BaseDao;

/**
 * Base class for JDBC-based DAOs.
 * 
 * <p>
 * Note: {@link BaseJdbcDao} utilizes Spring's {@link JdbcTemplate} to query
 * data.
 * </p>
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class BaseJdbcDao extends BaseDao {
    private DataSource dataSource;

    public BaseJdbcDao setDataSource(DataSource ds) {
        this.dataSource = ds;
        return this;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Obtains a {@code Connection} instance.
     * 
     * @return
     * @throws SQLException
     */
    protected Connection connection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Gets {@link JdbcTemplate} instance for a given {@link Connection}.
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
     * Gets a {@link JdbcTemplate} instance.
     * 
     * @return
     */
    protected JdbcTemplate jdbcTemplate() {
        return new JdbcTemplate(dataSource);
    }

    /*--------------------------------------------------------------------------------*/

    /**
     * Executes a non-SELECT statement.
     * 
     * @param sql
     * @param paramValues
     * @return number of affected rows
     */
    protected int execute(String sql, Object[] paramValues) {
        return execute(jdbcTemplate(), sql, paramValues);
    }

    /**
     * Executes a non-SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param paramValues
     * @return number of affected rows
     */
    protected int execute(Connection conn, String sql, Object[] paramValues) {
        return execute(jdbcTemplate(conn), sql, paramValues);
    }

    /**
     * Executes a non-SELECT statement.
     * 
     * @param jdbcTemplate
     * @param sql
     * @param paramValues
     * @return number of affected rows
     */
    protected int execute(JdbcTemplate jdbcTemplate, String sql, Object[] paramValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            List<Object> params = new ArrayList<Object>();
            if (paramValues != null) {
                for (Object val : paramValues) {
                    if (!(val instanceof ParamExpression)) {
                        params.add(val);
                    }
                }
            }
            return params.size() > 0 ? jdbcTemplate.update(sql, params.toArray()) : jdbcTemplate
                    .update(sql);
        } finally {
            addProfiling(System.currentTimeMillis() - timestampStart, sql);
        }
    }

    /*--------------------------------------------------------------------------------*/

    /**
     * Executes a SELECT statement.
     * 
     * @param rowMapper
     * @param sql
     * @param paramValues
     * @return
     */
    protected <T> List<T> executeSelect(RowMapper<T> rowMapper, String sql, Object[] paramValues) {
        return executeSelect(rowMapper, jdbcTemplate(), sql, paramValues);
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param rowMapper
     * @param conn
     * @param sql
     * @param paramValues
     * @return
     */
    protected <T> List<T> executeSelect(RowMapper<T> rowMapper, Connection conn, String sql,
            Object[] paramValues) {
        return executeSelect(rowMapper, jdbcTemplate(conn), sql, paramValues);
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param rowMapper
     * @param jdbcTemplate
     * @param sql
     * @param paramValues
     * @return
     */
    protected <T> List<T> executeSelect(RowMapper<T> rowMapper, JdbcTemplate jdbcTemplate,
            String sql, Object[] paramValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            List<Object> params = new ArrayList<Object>();
            if (paramValues != null) {
                for (Object val : paramValues) {
                    if (!(val instanceof ParamExpression)) {
                        params.add(val);
                    }
                }
            }
            return params.size() > 0 ? jdbcTemplate.query(sql, rowMapper, params.toArray())
                    : jdbcTemplate.query(sql, rowMapper);
        } finally {
            addProfiling(System.currentTimeMillis() - timestampStart, sql);
        }
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param sql
     * @param paramValues
     * @return
     */
    protected List<Map<String, Object>> executeSelect(String sql, Object[] paramValues) {
        return executeSelect(jdbcTemplate(), sql, paramValues);
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param conn
     * @param sql
     * @param paramValues
     * @return
     */
    protected List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Object[] paramValues) {
        return executeSelect(jdbcTemplate(conn), sql, paramValues);
    }

    /**
     * Executes a SELECT statement.
     * 
     * @param jdbcTemplate
     * @param sql
     * @param paramValues
     * @return
     */
    protected List<Map<String, Object>> executeSelect(JdbcTemplate jdbcTemplate, String sql,
            Object[] paramValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            List<Object> params = new ArrayList<Object>();
            if (paramValues != null) {
                for (Object val : paramValues) {
                    if (!(val instanceof ParamExpression)) {
                        params.add(val);
                    }
                }
            }
            return params.size() > 0 ? jdbcTemplate.queryForList(sql, params.toArray())
                    : jdbcTemplate.queryForList(sql);
        } finally {
            addProfiling(System.currentTimeMillis() - timestampStart, sql);
        }
    }
}
