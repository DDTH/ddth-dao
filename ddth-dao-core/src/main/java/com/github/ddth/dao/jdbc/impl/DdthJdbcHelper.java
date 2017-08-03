package com.github.ddth.dao.jdbc.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.BaseDao;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.DbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.IRowMapper;

/**
 * Pure-JDBC implementation of {@link IJdbcHelper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class DdthJdbcHelper extends AbstractJdbcHelper {

    private final static Pattern PATTERN_NAMED_PARAM = Pattern.compile(":(\\w+)");
    // private final static String[] SINGLE_PLACEHOLDER = { "?" };

    // /**
    // * Build array of "?" placeholders according the supplied bind value.
    // *
    // * @param bindValue
    // * @return
    // */
    // private static String[] buildPlaceholders(Object bindValue) {
    // if (bindValue instanceof Object[]) {
    // Object[] objs = (Object[]) bindValue;
    // if (objs.length > 0) {
    // return Collections.nCopies(objs.length,
    // "?").toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    // }
    // } else if (bindValue instanceof List<?>) {
    // List<?> list = (List<?>) bindValue;
    // if (list.size() > 0) {
    // return Collections.nCopies(list.size(),
    // "?").toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    // }
    // }
    // return SINGLE_PLACEHOLDER;
    // }

    /**
     * Build array of final build values according the supplied build value.
     * 
     * @param bindValue
     * @return
     */
    private static Object[] buildBindValues(Object bindValue) {
        if (bindValue instanceof boolean[]) {
            boolean[] bools = (boolean[]) bindValue;
            return bools.length > 0 ? ArrayUtils.toObject(bools) : null;
        } else if (bindValue instanceof short[]) {
            short[] shorts = (short[]) bindValue;
            return shorts.length > 0 ? ArrayUtils.toObject(shorts) : null;
        } else if (bindValue instanceof int[]) {
            int[] ints = (int[]) bindValue;
            return ints.length > 0 ? ArrayUtils.toObject(ints) : null;
        } else if (bindValue instanceof long[]) {
            long[] longs = (long[]) bindValue;
            return longs.length > 0 ? ArrayUtils.toObject(longs) : null;
        } else if (bindValue instanceof float[]) {
            float[] floats = (float[]) bindValue;
            return floats.length > 0 ? ArrayUtils.toObject(floats) : null;
        } else if (bindValue instanceof double[]) {
            double[] doubles = (double[]) bindValue;
            return doubles.length > 0 ? ArrayUtils.toObject(doubles) : null;
        } else if (bindValue instanceof char[]) {
            char[] chars = (char[]) bindValue;
            return chars.length > 0 ? ArrayUtils.toObject(chars) : null;
        } else if (bindValue instanceof Object[]) {
            Object[] objs = (Object[]) bindValue;
            return objs.length > 0 ? objs : null;
        } else if (bindValue instanceof List<?>) {
            List<?> list = (List<?>) bindValue;
            return list.size() > 0 ? list.toArray() : null;
        }
        return new Object[] { bindValue };
    }

    /**
     * Prepare and bind values a named parameters statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @throws SQLException
     */
    protected PreparedStatement prepareAndBindNamedParamsStatement(Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException {
        List<Object> params = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        Matcher m = PATTERN_NAMED_PARAM.matcher(sql);
        while (m.find()) {
            String namedParam = m.group(1);
            Object bindValue = bindValues.get(namedParam);
            Object[] bindList = buildBindValues(bindValue);
            m.appendReplacement(sb, StringUtils.repeat("?", ",", bindList.length));
            for (Object bind : bindList) {
                params.add(bind);
            }
        }
        m.appendTail(sb);
        PreparedStatement pstm = conn.prepareStatement(sb.toString());
        DbcHelper.bindParams(pstm, params.toArray());
        return pstm;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Object... bindValues) throws SQLException {
        Connection conn = getConnection();
        try {
            return execute(conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(String sql, Map<String, ?> bindValues) throws SQLException {
        Connection conn = getConnection();
        try {
            return execute(conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Object... bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            try {
                DbcHelper.bindParams(pstm, bindValues);
                return pstm.executeUpdate();
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int execute(Connection conn, String sql, Map<String, ?> bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = prepareAndBindNamedParamsStatement(conn, sql, bindValues);
            try {
                return pstm.executeUpdate();
            } finally {
                pstm.close();
            }
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
            return executeSelect(rowMapper, conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelect(rowMapper, conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            try {
                DbcHelper.bindParams(pstm, bindValues);
                ResultSet rs = pstm.executeQuery();
                try {
                    List<T> result = new ArrayList<>();
                    int rowNum = 0;
                    while (rs.next()) {
                        result.add(rowMapper.mapRow(rs, rowNum));
                        rowNum++;
                    }
                    return result;
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> List<T> executeSelect(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = prepareAndBindNamedParamsStatement(conn, sql, bindValues);
            try {
                ResultSet rs = pstm.executeQuery();
                try {
                    List<T> result = new ArrayList<>();
                    int rowNum = 0;
                    while (rs.next()) {
                        result.add(rowMapper.mapRow(rs, rowNum));
                        rowNum++;
                    }
                    return result;
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
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
            return executeSelect(conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(String sql, Map<String, ?> bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelect(conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Object... bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            try {
                DbcHelper.bindParams(pstm, bindValues);
                ResultSet rs = pstm.executeQuery();
                try {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int numColumns = rsmd.getColumnCount();
                    String[] colLabels = new String[numColumns];
                    for (int i = 0; i < numColumns; i++) {
                        colLabels[i] = rsmd.getColumnName(i + 1);
                    }

                    List<Map<String, Object>> result = new ArrayList<>();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        result.add(row);
                        for (int i = 0; i < numColumns; i++) {
                            row.put(colLabels[i], rs.getObject(colLabels[i]));
                        }
                    }
                    return result;
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Map<String, Object>> executeSelect(Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = prepareAndBindNamedParamsStatement(conn, sql, bindValues);
            try {
                ResultSet rs = pstm.executeQuery();
                try {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int numColumns = rsmd.getColumnCount();
                    String[] colLabels = new String[numColumns];
                    for (int i = 0; i < numColumns; i++) {
                        colLabels[i] = rsmd.getColumnName(i + 1);
                    }

                    List<Map<String, Object>> result = new ArrayList<>();
                    while (rs.next()) {
                        Map<String, Object> row = new HashMap<>();
                        result.add(row);
                        for (int i = 0; i < numColumns; i++) {
                            row.put(colLabels[i], rs.getObject(colLabels[i]));
                        }
                    }
                    return result;
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Object... bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelectOne(rowMapper, conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, String sql, Map<String, ?> bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelectOne(rowMapper, conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Object... bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            try {
                DbcHelper.bindParams(pstm, bindValues);
                pstm.setFetchSize(1);
                ResultSet rs = pstm.executeQuery();
                try {
                    if (rs.next()) {
                        return rowMapper.mapRow(rs, 0);
                    } else {
                        return null;
                    }
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T executeSelectOne(IRowMapper<T> rowMapper, Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = prepareAndBindNamedParamsStatement(conn, sql, bindValues);
            try {
                pstm.setFetchSize(1);
                ResultSet rs = pstm.executeQuery();
                try {
                    if (rs.next()) {
                        return rowMapper.mapRow(rs, 0);
                    } else {
                        return null;
                    }
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Object... bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelectOne(conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(String sql, Map<String, ?> bindValues)
            throws SQLException {
        Connection conn = getConnection();
        try {
            return executeSelectOne(conn, sql, bindValues);
        } finally {
            returnConnection(conn);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql, Object... bindValues)
            throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = conn.prepareStatement(sql);
            try {
                DbcHelper.bindParams(pstm, bindValues);
                pstm.setFetchSize(1);
                ResultSet rs = pstm.executeQuery();
                try {
                    if (rs.next()) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int numColumns = rsmd.getColumnCount();
                        String[] colLabels = new String[numColumns];
                        for (int i = 0; i < numColumns; i++) {
                            colLabels[i] = rsmd.getColumnName(i + 1);
                        }

                        Map<String, Object> row = new HashMap<>();
                        for (int i = 0; i < numColumns; i++) {
                            row.put(colLabels[i], rs.getObject(colLabels[i]));
                        }
                        return row;
                    }
                    return null;
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> executeSelectOne(Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException {
        long timestampStart = System.currentTimeMillis();
        try {
            PreparedStatement pstm = prepareAndBindNamedParamsStatement(conn, sql, bindValues);
            try {
                pstm.setFetchSize(1);
                ResultSet rs = pstm.executeQuery();
                try {
                    if (rs.next()) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        int numColumns = rsmd.getColumnCount();
                        String[] colLabels = new String[numColumns];
                        for (int i = 0; i < numColumns; i++) {
                            colLabels[i] = rsmd.getColumnName(i + 1);
                        }

                        Map<String, Object> row = new HashMap<>();
                        for (int i = 0; i < numColumns; i++) {
                            row.put(colLabels[i], rs.getObject(colLabels[i]));
                        }
                        return row;
                    }
                    return null;
                } finally {
                    rs.close();
                }
            } finally {
                pstm.close();
            }
        } finally {
            BaseDao.addProfiling(timestampStart, sql, System.currentTimeMillis() - timestampStart);
        }
    }

    public static void main(String[] args) {
        Map<String, ?> bindValues = MapUtils.createMap("id", new int[] { 1, 2, 3 }, "name",
                "Nguyễn Bá Thành");
        String SQL = "SELECT * FROM table WHERE name=:name OR id IN (:id)";

        List<Object> params = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        Matcher m = PATTERN_NAMED_PARAM.matcher(SQL);
        while (m.find()) {
            String namedParam = m.group(1);
            Object bindValue = bindValues.get(namedParam);
            Object[] bindList = buildBindValues(bindValue);
            m.appendReplacement(sb, StringUtils.repeat("?", ",", bindList.length));
            for (Object bind : bindList) {
                params.add(bind);
            }
        }
        m.appendTail(sb);
        System.out.println(sb);
        System.out.println(params);
    }
}
