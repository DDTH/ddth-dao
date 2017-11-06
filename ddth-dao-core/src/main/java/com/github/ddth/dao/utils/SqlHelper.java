package com.github.ddth.dao.utils;

import java.sql.PreparedStatement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.dao.jdbc.ParamExpression;

/**
 * Helper class to build SQLs.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class SqlHelper {
    /**
     * Builds a DELETE statement (used for {@link PreparedStatement}).
     * 
     * @param tableName
     * @param whereColumns
     * @param whereValues
     * @return
     */
    public static String buildSqlDELETE(String tableName, String[] whereColumns,
            Object[] whereValues) {
        final String SQL_TEMPLATE_FULL = "DELETE FROM {0} WHERE {1}";
        final String SQL_TEMPLATE = "DELETE FROM {0}";

        final List<String> WHERE_CLAUSE = new ArrayList<String>();
        if (whereColumns != null && whereColumns.length > 0 && whereValues != null
                && whereValues.length > 0) {
            if (whereColumns.length != whereValues.length) {
                throw new IllegalArgumentException(
                        "Number of whereColumns must be equal to number of whereValues.");
            }
            for (int i = 0; i < whereColumns.length; i++) {
                if (whereValues[i] instanceof ParamExpression) {
                    WHERE_CLAUSE.add("(" + whereColumns[i] + "="
                            + ((ParamExpression) whereValues[i]).getExpression() + ")");
                } else {
                    WHERE_CLAUSE.add("(" + whereColumns[i] + "=?)");
                }
            }
        }

        String SQL;
        if (WHERE_CLAUSE.size() > 0) {
            SQL = MessageFormat.format(SQL_TEMPLATE_FULL, tableName,
                    StringUtils.join(WHERE_CLAUSE, " AND "));
        } else {
            SQL = MessageFormat.format(SQL_TEMPLATE, tableName);
        }
        return SQL;
    }

    /**
     * Builds an INSERT statement (used for {@link PreparedStatement}).
     * 
     * @param tableName
     * @param columnNames
     * @param values
     * @return
     */
    public static String buildSqlINSERT(String tableName, String[] columnNames, Object[] values) {
        if (columnNames.length != values.length) {
            throw new IllegalArgumentException(
                    "Number of columns must be equal to number of values.");
        }
        final String SQL_TEMPLATE = "INSERT INTO {0} ({1}) VALUES ({2})";
        final String SQL_PART_COLUMNS = StringUtils.join(columnNames, ',');
        final StringBuilder SQL_PART_VALUES = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
            if (values[i] instanceof ParamExpression) {
                SQL_PART_VALUES.append(((ParamExpression) values[i]).getExpression());
            } else {
                SQL_PART_VALUES.append('?');
            }
            if (i < values.length - 1) {
                SQL_PART_VALUES.append(',');
            }
        }
        return MessageFormat.format(SQL_TEMPLATE, tableName, SQL_PART_COLUMNS, SQL_PART_VALUES);
    }

    /**
     * Builds a SELECT statement (used for {@link PreparedStatement}).
     * 
     * @param tableName
     * @param columns
     * @param whereColumns
     * @param whereValues
     * @return
     */
    public static String buildSqlSELECT(String tableName, String[][] columns, String[] whereColumns,
            Object[] whereValues) {
        final String SQL_TEMPLATE_WHERE = "SELECT {1} FROM {0} WHERE {2}";
        final String SQL_TEMPLATE = "SELECT {1} FROM {0}";

        final List<String> WHERE_CLAUSE = new ArrayList<String>();
        if (whereColumns != null && whereValues != null) {
            if (whereColumns.length != whereValues.length) {
                throw new IllegalArgumentException(
                        "Number of whereColumns must be equal to number of whereValues.");
            }
            for (int i = 0; i < whereColumns.length; i++) {
                if (whereValues[i] instanceof ParamExpression) {
                    WHERE_CLAUSE.add("(" + whereColumns[i] + "="
                            + ((ParamExpression) whereValues[i]).getExpression() + ")");
                } else {
                    WHERE_CLAUSE.add("(" + whereColumns[i] + "=?)");
                }
            }
        }

        final List<String> SELECT_COLUMNS = new ArrayList<String>();
        for (String[] colDef : columns) {
            if (colDef.length > 1) {
                SELECT_COLUMNS.add(colDef[0] + " AS " + colDef[1]);
            } else {
                SELECT_COLUMNS.add(colDef[0]);
            }
        }

        String SQL;
        if (WHERE_CLAUSE.size() > 0) {
            SQL = MessageFormat.format(SQL_TEMPLATE_WHERE, tableName,
                    StringUtils.join(SELECT_COLUMNS, ", "),
                    StringUtils.join(WHERE_CLAUSE, " AND "));
        } else {
            SQL = MessageFormat.format(SQL_TEMPLATE, tableName,
                    StringUtils.join(SELECT_COLUMNS, ", "));
        }

        return SQL;
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
     */
    public String buildSqlUPDATE(String tableName, String[] columnNames, Object[] values,
            String[] whereColumns, Object[] whereValues) {
        final String SQL_TEMPLATE_FULL = "UPDATE {0} SET {1} WHERE {2}";
        final String SQL_TEMPLATE = "UPDATE {0} SET {1}";

        if (columnNames.length != values.length) {
            throw new IllegalArgumentException(
                    "Number of columns must be equal to number of values.");
        }
        final List<String> UPDATE_CLAUSE = new ArrayList<String>();
        for (int i = 0; i < columnNames.length; i++) {
            if (values[i] instanceof ParamExpression) {
                UPDATE_CLAUSE
                        .add(columnNames[i] + "=" + ((ParamExpression) values[i]).getExpression());
            } else {
                UPDATE_CLAUSE.add(columnNames[i] + "=?");
            }
        }

        final List<String> WHERE_CLAUSE = new ArrayList<String>();
        if (whereColumns != null && whereColumns.length > 0 && whereValues != null
                && whereValues.length > 0) {
            if (whereColumns.length != whereValues.length) {
                throw new IllegalArgumentException(
                        "Number of whereColumns must be equal to number of whereValues.");
            }
            for (int i = 0; i < whereColumns.length; i++) {
                if (whereValues[i] instanceof ParamExpression) {
                    WHERE_CLAUSE.add("(" + whereColumns[i] + "="
                            + ((ParamExpression) whereValues[i]).getExpression() + ")");
                } else {
                    WHERE_CLAUSE.add("(" + whereColumns[i] + "=?)");
                }
            }
        }

        String SQL;
        if (WHERE_CLAUSE.size() > 0) {
            SQL = MessageFormat.format(SQL_TEMPLATE_FULL, tableName,
                    StringUtils.join(UPDATE_CLAUSE, ','), StringUtils.join(WHERE_CLAUSE, " AND "));
        } else {
            SQL = MessageFormat.format(SQL_TEMPLATE, tableName,
                    StringUtils.join(UPDATE_CLAUSE, ','));
        }

        return SQL;
    }
}
