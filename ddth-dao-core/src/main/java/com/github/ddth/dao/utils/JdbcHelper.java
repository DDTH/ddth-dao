package com.github.ddth.dao.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Jdbc Helper class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.2
 */
public class JdbcHelper {
    /**
     * Extract column label/name from a {@link ResultSet}.
     * 
     * @param rs
     * @return
     * @throws SQLException
     */
    public static String[] extractColumnLabels(ResultSet rs) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int numColumns = rsmd.getColumnCount();
        String[] colLabels = new String[numColumns];
        for (int i = 1; i <= numColumns; i++) {
            colLabels[i - 1] = rsmd.getColumnLabel(i);
            if (colLabels[i - 1] == null) {
                colLabels[i - 1] = rsmd.getColumnName(i);
            }
        }
        return colLabels;
    }

    /**
     * Bind parameter values to a {@link PreparedStatement}.
     * 
     * @param pstm
     * @param bindValues
     * @return
     * @since 0.8.0
     * @throws SQLException
     */
    public static PreparedStatement bindParams(PreparedStatement pstm, Object... bindValues)
            throws SQLException {
        if (bindValues != null)
            for (int i = 0; i < bindValues.length; i++) {
                Object value = bindValues[i];
                if (value instanceof Boolean) {
                    pstm.setBoolean(i + 1, (Boolean) value);
                } else if (value instanceof Character || value instanceof String) {
                    pstm.setString(i + 1, value.toString());
                } else if (value instanceof Byte) {
                    pstm.setByte(i + 1, (Byte) value);
                } else if (value instanceof Short) {
                    pstm.setShort(i + 1, (Short) value);
                } else if (value instanceof Integer) {
                    pstm.setInt(i + 1, (Integer) value);
                } else if (value instanceof Long) {
                    pstm.setLong(i + 1, (Long) value);
                } else if (value instanceof BigInteger) {
                    pstm.setLong(i + 1, ((BigInteger) value).longValue());
                } else if (value instanceof Float) {
                    pstm.setFloat(i + 1, (Float) value);
                } else if (value instanceof Double) {
                    pstm.setDouble(i + 1, (Double) value);
                } else if (value instanceof BigDecimal) {
                    pstm.setBigDecimal(i + 1, (BigDecimal) value);
                } else if (value instanceof java.sql.Date) {
                    pstm.setDate(i + 1, (java.sql.Date) value);
                } else if (value instanceof java.sql.Time) {
                    pstm.setTime(i + 1, (java.sql.Time) value);
                } else if (value instanceof java.sql.Timestamp) {
                    pstm.setTimestamp(i + 1, (java.sql.Timestamp) value);
                } else if (value instanceof Date) {
                    pstm.setTimestamp(i + 1, new Timestamp(((Date) value).getTime()));
                } else if (value instanceof Blob) {
                    pstm.setBlob(i + 1, (Blob) value);
                } else if (value instanceof Clob) {
                    pstm.setClob(i + 1, (Clob) value);
                } else if (value instanceof byte[]) {
                    pstm.setBytes(i + 1, (byte[]) value);
                } else {
                    pstm.setObject(i + 1, value);
                }
            }
        return pstm;
    }

    /**
     * Bind parameter values to a {@link CallableStatement}.
     * 
     * @param cstm
     * @param bindValues
     * @return
     * @since 0.8.2
     * @throws SQLException
     */
    public static PreparedStatement bindParams(CallableStatement cstm, Object... bindValues)
            throws SQLException {
        if (bindValues != null)
            for (int i = 0; i < bindValues.length; i++) {
                Object value = bindValues[i];
                if (value instanceof Boolean) {
                    cstm.setBoolean(i + 1, (Boolean) value);
                } else if (value instanceof Character || value instanceof String) {
                    cstm.setString(i + 1, value.toString());
                } else if (value instanceof Byte) {
                    cstm.setByte(i + 1, (Byte) value);
                } else if (value instanceof Short) {
                    cstm.setShort(i + 1, (Short) value);
                } else if (value instanceof Integer) {
                    cstm.setInt(i + 1, (Integer) value);
                } else if (value instanceof Long) {
                    cstm.setLong(i + 1, (Long) value);
                } else if (value instanceof BigInteger) {
                    cstm.setLong(i + 1, ((BigInteger) value).longValue());
                } else if (value instanceof Float) {
                    cstm.setFloat(i + 1, (Float) value);
                } else if (value instanceof Double) {
                    cstm.setDouble(i + 1, (Double) value);
                } else if (value instanceof BigDecimal) {
                    cstm.setBigDecimal(i + 1, (BigDecimal) value);
                } else if (value instanceof java.sql.Date) {
                    cstm.setDate(i + 1, (java.sql.Date) value);
                } else if (value instanceof java.sql.Time) {
                    cstm.setTime(i + 1, (java.sql.Time) value);
                } else if (value instanceof java.sql.Timestamp) {
                    cstm.setTimestamp(i + 1, (java.sql.Timestamp) value);
                } else if (value instanceof Date) {
                    cstm.setTimestamp(i + 1, new Timestamp(((Date) value).getTime()));
                } else if (value instanceof Blob) {
                    cstm.setBlob(i + 1, (Blob) value);
                } else if (value instanceof Clob) {
                    cstm.setClob(i + 1, (Clob) value);
                } else if (value instanceof byte[]) {
                    cstm.setBytes(i + 1, (byte[]) value);
                } else {
                    cstm.setObject(i + 1, value);
                }
            }
        return cstm;
    }

    private final static Pattern PATTERN_NAMED_PARAM = Pattern.compile(":(\\w+)");

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

    private static String buildStatementAndValues(String sql, Map<String, ?> bindValues,
            List<Object> outValues) {
        if (bindValues == null) {
            bindValues = new HashMap<>();
        }
        StringBuffer sb = new StringBuffer();
        Matcher m = PATTERN_NAMED_PARAM.matcher(sql);
        while (m.find()) {
            String namedParam = m.group(1);
            Object bindValue = bindValues.get(namedParam);
            Object[] bindList = buildBindValues(bindValue);
            m.appendReplacement(sb, StringUtils.repeat("?", ",", bindList.length));
            for (Object bind : bindList) {
                outValues.add(bind);
            }
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * Prepare and bind parameter values a named-parameter statement.
     * 
     * @param conn
     * @param sql
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.2
     * @throws SQLException
     */
    public static PreparedStatement prepareAndBindNamedParamsStatement(Connection conn, String sql,
            Map<String, ?> bindValues) throws SQLException {
        List<Object> indexBindValues = new ArrayList<>();
        String finalSql = buildStatementAndValues(sql, bindValues, indexBindValues);
        PreparedStatement pstm = conn.prepareStatement(finalSql);
        bindParams(pstm, indexBindValues.toArray());
        return pstm;
    }

    /**
     * Prepare and bind parameter values a named-parameter statement.
     * 
     * @param conn
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.2
     * @throws SQLException
     */
    public static PreparedStatement prepareAndBindNamedParamsStatement(Connection conn, String sql,
            int resultSetType, int resultSetConcurrency, Map<String, ?> bindValues)
            throws SQLException {
        List<Object> indexBindValues = new ArrayList<>();
        String finalSql = buildStatementAndValues(sql, bindValues, indexBindValues);
        PreparedStatement pstm = conn.prepareStatement(finalSql, resultSetType,
                resultSetConcurrency);
        bindParams(pstm, indexBindValues.toArray());
        return pstm;
    }

    /**
     * Prepare and bind parameter values a named-parameter statement.
     * 
     * @param conn
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @param resultSetHoldability
     * @param bindValues
     *            name-based bind values
     * @return
     * @since 0.8.2
     * @throws SQLException
     */
    public static PreparedStatement prepareAndBindNamedParamsStatement(Connection conn, String sql,
            int resultSetType, int resultSetConcurrency, int resultSetHoldability,
            Map<String, ?> bindValues) throws SQLException {
        List<Object> indexBindValues = new ArrayList<>();
        String finalSql = buildStatementAndValues(sql, bindValues, indexBindValues);
        PreparedStatement pstm = conn.prepareStatement(finalSql, resultSetType,
                resultSetConcurrency, resultSetHoldability);
        bindParams(pstm, indexBindValues.toArray());
        return pstm;
    }
}
