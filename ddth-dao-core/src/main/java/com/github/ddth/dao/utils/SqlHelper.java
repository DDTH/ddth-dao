package com.github.ddth.dao.utils;

import com.github.ddth.dao.jdbc.utils.DefaultFilters;
import com.github.ddth.dao.jdbc.utils.DefaultSqlBuilders;
import com.github.ddth.dao.jdbc.utils.IFilter;
import com.github.ddth.dao.jdbc.utils.ISqlBuilder;

import java.sql.PreparedStatement;

/**
 * Helper class to build SQLs.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 * @deprecated since 1.0.0 this class is kept for backward compatibility only, adn will be removed in the future. Use {@link ISqlBuilder} as replacement.
 */
public class SqlHelper {
    private static IFilter buildWhereFilter(String[] whereColumns, Object[] whereValues) {
        DefaultFilters.FilterAnd filter = null;
        if (whereColumns != null && whereColumns.length > 0) {
            if (whereValues == null || whereColumns.length != whereValues.length) {
                throw new IllegalArgumentException("Number of where-columns must be equal to number of where-values.");
            }
            filter = new DefaultFilters.FilterAnd();
            for (int i = 0; i < whereColumns.length; i++) {
                filter.addFilter(new DefaultFilters.FilterFieldValue(whereColumns[i], "=", whereValues[i]));
            }
        }
        return filter;
    }

    /**
     * Builds a DELETE statement (used for {@link PreparedStatement}).
     *
     * @param tableName
     * @param whereColumns
     * @param whereValues
     * @return
     * @see com.github.ddth.dao.jdbc.utils.DefaultSqlBuilders.DeleteBuilder
     * @deprecated since 1.0.0, use {@link com.github.ddth.dao.jdbc.utils.ISqlBuilder}
     */
    public static String buildSqlDELETE(String tableName, String[] whereColumns, Object[] whereValues) {
        IFilter filter = buildWhereFilter(whereColumns, whereValues);
        ISqlBuilder builder = new DefaultSqlBuilders.DeleteBuilder(tableName, filter);
        return builder.build().clause;
    }

    /**
     * Builds an INSERT statement (used for {@link PreparedStatement}).
     *
     * @param tableName
     * @param columnNames
     * @param values
     * @return
     * @see com.github.ddth.dao.jdbc.utils.DefaultSqlBuilders.InsertBuilder
     * @deprecated since 1.0.0, use {@link com.github.ddth.dao.jdbc.utils.ISqlBuilder}
     */
    public static String buildSqlINSERT(String tableName, String[] columnNames, Object[] values) {
        if (columnNames.length != values.length) {
            throw new IllegalArgumentException("Number of columns must be equal to number of values.");
        }
        ISqlBuilder builder = new DefaultSqlBuilders.InsertBuilder(tableName);
        for (int i = 0; i < columnNames.length; i++) {
            builder = ((DefaultSqlBuilders.InsertBuilder) builder).addValue(columnNames[i], values[i]);
        }
        return builder.build().clause;
    }

    /**
     * Builds a SELECT statement (used for {@link PreparedStatement}).
     *
     * @param tableName
     * @param columns
     * @param whereColumns
     * @param whereValues
     * @return
     * @see com.github.ddth.dao.jdbc.utils.DefaultSqlBuilders.SelectBuilder
     * @deprecated since 1.0.0, use {@link com.github.ddth.dao.jdbc.utils.ISqlBuilder}
     */
    public static String buildSqlSELECT(String tableName, String[][] columns, String[] whereColumns,
            Object[] whereValues) {
        IFilter filter = buildWhereFilter(whereColumns, whereValues);
        ISqlBuilder builder = new DefaultSqlBuilders.SelectBuilder().withTables(tableName);
        for (String[] colDef : columns) {
            if (colDef.length > 1) {
                ((DefaultSqlBuilders.SelectBuilder) builder).addColumn(colDef[0] + " AS " + colDef[1]);
            } else {
                ((DefaultSqlBuilders.SelectBuilder) builder).addColumn(colDef[0]);
            }
        }
        if (filter != null) {
            ((DefaultSqlBuilders.SelectBuilder) builder).withFilterWhere(filter);
        }
        return builder.build().clause;
    }

    /**
     * Builds an UPDATE statement (used for {@link PreparedStatement}).
     *
     * @param tableName
     * @param columnNames
     * @param values
     * @param whereColumns
     * @param whereValues
     * @return
     * @see com.github.ddth.dao.jdbc.utils.DefaultSqlBuilders.UpdateBuilder
     * @deprecated since 1.0.0, use {@link com.github.ddth.dao.jdbc.utils.ISqlBuilder}
     */
    public static String buildSqlUPDATE(String tableName, String[] columnNames, Object[] values, String[] whereColumns,
            Object[] whereValues) {
        IFilter filter = buildWhereFilter(whereColumns, whereValues);
        if (columnNames.length != values.length) {
            throw new IllegalArgumentException("Number of columns must be equal to number of values.");
        }
        ISqlBuilder builder = new DefaultSqlBuilders.UpdateBuilder(tableName);
        for (int i = 0; i < columnNames.length; i++) {
            builder = ((DefaultSqlBuilders.UpdateBuilder) builder).addValue(columnNames[i], values[i]);
        }
        if (filter != null) {
            ((DefaultSqlBuilders.UpdateBuilder) builder).withFilter(filter);
        }
        return builder.build().clause;
    }
}
