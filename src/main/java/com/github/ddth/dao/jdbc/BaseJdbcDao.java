package com.github.ddth.dao.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

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
    private String id = UUID.randomUUID().toString();
    private DataSource dataSource;

    public BaseJdbcDao setDataSource(DataSource ds) {
        this.dataSource = ds;
        return this;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.2.0
     */
    @Override
    public BaseJdbcDao init() {
        DbcHelper.registerJdbcDataSource(id, dataSource);
        return (BaseJdbcDao) super.init();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.2.0
     */
    @Override
    public void destroy() {
        try {
            super.destroy();
        } finally {
            DbcHelper.unregisterJdbcDataSource(id);
        }
    }

    /**
     * Obtains a {@link Connection} instance, without transaction (
     * {@code autoCommit=false}).
     * 
     * @return
     * @throws SQLException
     */
    protected Connection connection() throws SQLException {
        return connection(false);
    }

    /**
     * Obtains a {@link Connection} instance, starts a transaction if specified.
     * 
     * @param startTransaction
     * @return
     * @throws SQLException
     * @since 0.2.0
     */
    protected Connection connection(boolean startTransaction) throws SQLException {
        return DbcHelper.getConnection(id, startTransaction);
    }

    /**
     * Returned a previously obtained {@link Connection}.
     * 
     * @param conn
     * @throws SQLException
     * @since 0.2.0
     */
    protected void returnConnection(Connection conn) throws SQLException {
        DbcHelper.returnConnection(conn);
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
     * @return SQLException
     */
    protected int execute(String sql, Object[] paramValues) throws SQLException {
        Connection conn = connection();
        try {
            return execute(jdbcTemplate(conn), sql, paramValues);
        } finally {
            returnConnection(conn);
        }
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
     * @throws SQLException
     */
    protected <T> List<T> executeSelect(RowMapper<T> rowMapper, String sql, Object[] paramValues)
            throws SQLException {
        Connection conn = connection();
        try {
            return executeSelect(rowMapper, jdbcTemplate(conn), sql, paramValues);
        } finally {
            returnConnection(conn);
        }
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
     * @throws SQLException
     */
    protected List<Map<String, Object>> executeSelect(String sql, Object[] paramValues)
            throws SQLException {
        Connection conn = connection();
        try {
            return executeSelect(jdbcTemplate(conn), sql, paramValues);
        } finally {
            returnConnection(conn);
            ;
        }
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
