package com.github.ddth.dao.jdbc.jdbctemplate;

import java.sql.Connection;
import java.sql.ResultSet;
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
import com.github.ddth.dao.jdbc.DbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.jdbc.ParamExpression;

/**
 * This implementation of {@link IJdbcHelper} utilizes Spring's
 * {@link JdbcTemplate} to interact with database.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class JdbcTemplateJdbcHelper implements IJdbcHelper {

    private String id = UUID.randomUUID().toString();
    private DataSource dataSource;

    public JdbcTemplateJdbcHelper setDataSource(DataSource ds) {
        this.dataSource = ds;
        return this;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Initializing method.
     * 
     * @return
     */
    public JdbcTemplateJdbcHelper init() {
        DbcHelper.registerJdbcDataSource(id, dataSource);
        return this;
    }

    /**
     * Destroying method.
     */
    public void destroy() {
        DbcHelper.unregisterJdbcDataSource(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(boolean startTransaction) throws SQLException {
        return DbcHelper.getConnection(id, startTransaction);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void returnConnection(Connection conn) throws SQLException {
        DbcHelper.returnConnection(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean startTransaction(Connection conn) throws SQLException {
        return DbcHelper.startTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean commitTransaction(Connection conn) throws SQLException {
        return DbcHelper.commitTransaction(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean rollbackTransaction(Connection conn) throws SQLException {
        return DbcHelper.rollbackTransaction(conn);
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
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Object... bindValues) throws SQLException {
        Connection conn = getConnection();
        try {
            return execute(jdbcTemplate(conn), sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Object... bindValues) throws SQLException {
        return execute(jdbcTemplate(conn), sql, bindValues);
    }

    /**
     * Executes a non-SELECT statement.
     * 
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     * @return number of affected rows
     */
    private int execute(JdbcTemplate jdbcTemplate, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            List<Object> params = new ArrayList<Object>();
            if (bindValues != null) {
                for (Object val : bindValues) {
                    if (!(val instanceof ParamExpression)) {
                        params.add(val);
                    }
                }
            }
            return params.size() > 0 ? jdbcTemplate.update(sql, params.toArray())
                    : jdbcTemplate.update(sql);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Object... bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelect(rowMapper, jdbcTemplate(conn), sql, bindValues);
        } finally {
            returnConnection(conn);
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
     * Executes a SELECT statement.
     * 
     * @param rowMapper
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     * @return
     */
    private <T> List<T> executeSelect(IRowMapper<T> rowMapper, JdbcTemplate jdbcTemplate,
            String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            List<Object> params = new ArrayList<Object>();
            if (bindValues != null) {
                for (Object val : bindValues) {
                    if (!(val instanceof ParamExpression)) {
                        params.add(val);
                    }
                }
            }
            RowMapper<T> jRowMapper = new RowMapper<T>() {
                @Override
                public T mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rowMapper.mapRow(rs, rowNum);
                }
            };
            return params.size() > 0 ? jdbcTemplate.query(sql, jRowMapper, params.toArray())
                    : jdbcTemplate.query(sql, jRowMapper);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Object... bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelect(jdbcTemplate(conn), sql, bindValues);
        } finally {
            returnConnection(conn);
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
     * Executes a SELECT statement.
     * 
     * @param jdbcTemplate
     * @param sql
     * @param bindValues
     * @return
     */
    private List<Map<String, Object>> executeSelect(JdbcTemplate jdbcTemplate, String sql,
            Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            List<Object> params = new ArrayList<Object>();
            if (bindValues != null) {
                for (Object val : bindValues) {
                    if (!(val instanceof ParamExpression)) {
                        params.add(val);
                    }
                }
            }
            return params.size() > 0 ? jdbcTemplate.queryForList(sql, params.toArray())
                    : jdbcTemplate.queryForList(sql);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }
}
