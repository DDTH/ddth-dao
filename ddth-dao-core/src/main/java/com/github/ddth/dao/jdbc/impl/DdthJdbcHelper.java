package com.github.ddth.dao.jdbc.impl;

import com.github.ddth.dao.BaseDao;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.IRowMapper;
import com.github.ddth.dao.utils.JdbcHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pure-JDBC implementation of {@link IJdbcHelper}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class DdthJdbcHelper extends AbstractJdbcHelper {

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            try (PreparedStatement pstm = conn.prepareStatement(sql)) {
                JdbcHelper.bindParams(pstm, bindValues);
                return pstm.executeUpdate();
            }
        } catch (SQLException e) {
            throw translateSQLException(conn, "execute", sql, e);
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
            try (PreparedStatement pstm = JdbcHelper.prepareAndBindNamedParamsStatement(conn, sql, bindValues)) {
                return pstm.executeUpdate();
            }
        } catch (SQLException e) {
            throw translateSQLException(conn, "execute", sql, e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    private <T> List<T> _executeSelect(IRowMapper<T> rowMapper, PreparedStatement pstm) throws SQLException {
        int fetchSize = getDefaultFetchSize();
        pstm.setFetchSize(fetchSize < 0 ? Integer.MIN_VALUE : fetchSize);
        try (ResultSet rs = pstm.executeQuery()) {
            List<T> result = new ArrayList<>();
            int rowNum = 0;
            while (rs.next()) {
                result.add(rowMapper.mapRow(rs, rowNum));
                rowNum++;
            }
            return result;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql, Object... bindValues) {
        long timestampStart = System.currentTimeMillis();
        try {
            try (PreparedStatement pstm = conn
                    .prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                JdbcHelper.bindParams(pstm, bindValues);
                return _executeSelect(rowMapper, pstm);
            }
        } catch (SQLException e) {
            throw translateSQLException(conn, "executeSelect", sql, e);
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
            try (PreparedStatement pstm = JdbcHelper
                    .prepareAndBindNamedParamsStatement(conn, sql, ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY, bindValues)) {
                return _executeSelect(rowMapper, pstm);
            }
        } catch (SQLException e) {
            throw translateSQLException(conn, "executeSelect", sql, e);
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }
}
