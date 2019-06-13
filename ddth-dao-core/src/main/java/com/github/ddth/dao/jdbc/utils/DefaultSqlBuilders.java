package com.github.ddth.dao.jdbc.utils;

import com.github.ddth.dao.utils.DatabaseVendor;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.*;

/**
 * Default implementations of {@link ISqlBuilder}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class DefaultSqlBuilders {
    /**
     * Build SQL statement that queries on one single database table.
     */
    public static abstract class SingleTableBuilder implements ISqlBuilder {
        protected DatabaseVendor vendor = DatabaseVendor.UNKNOWN;
        protected String tableName;

        public SingleTableBuilder(String tableName) {
            this.tableName = tableName;
        }

        public SingleTableBuilder withVendor(DatabaseVendor vendor) {
            this.vendor = vendor != null ? vendor : DatabaseVendor.UNKNOWN;
            return this;
        }
    }

    /**
     * Build SQL statement that queries on multiple database tables.
     */
    public static abstract class MultipleTablesBuilder implements ISqlBuilder {
        protected DatabaseVendor vendor = DatabaseVendor.UNKNOWN;
        protected List<String> tableNames = new ArrayList<>();

        public MultipleTablesBuilder() {
        }

        public MultipleTablesBuilder(Collection<String> tableNames) {
            withTables(tableNames);
        }

        public MultipleTablesBuilder withVendor(DatabaseVendor vendor) {
            this.vendor = vendor != null ? vendor : DatabaseVendor.UNKNOWN;
            return this;
        }

        public MultipleTablesBuilder withTables(Collection<String> tableNames) {
            this.tableNames.clear();
            if (tableNames != null) {
                this.tableNames.addAll(tableNames);
            }
            return this;
        }

        public MultipleTablesBuilder withTables(String... tableNames) {
            this.tableNames.clear();
            if (tableNames != null) {
                for (String tableName : tableNames) {
                    this.tableNames.add(tableName);
                }
            }
            return this;
        }

        public MultipleTablesBuilder addTable(String tableName) {
            this.tableNames.add(tableName);
            return this;
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link ISqlBuilder} that builds DELETE statement.
     *
     * <p>Built statement: {@code DELETE FROM <table-name> [WHERE <filter>]}</p>
     */
    public static class DeleteBuilder extends SingleTableBuilder {
        private IFilter filter;

        public DeleteBuilder(String tableName, IFilter filter) {
            super(tableName);
            this.filter = filter;
        }

        @Override
        public BuildSqlResult build() {
            final String SQL_TEMPLATE_FULL = "DELETE FROM {0} WHERE {1}";
            final String SQL_TEMPLATE = "DELETE FROM {0}";
            if (filter == null) {
                return new BuildSqlResult(MessageFormat.format(SQL_TEMPLATE, tableName));
            } else {
                BuildSqlResult whereClause = filter.build();
                return new BuildSqlResult(MessageFormat.format(SQL_TEMPLATE_FULL, tableName, whereClause.clause),
                        whereClause.bindValues);
            }
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link ISqlBuilder} that builds INSERT statement.
     *
     * <p>Built statement: {@code INSERT INTO <table-name> (<column-list>) VALUES (<placeholders>)}</p>
     */
    public static class InsertBuilder extends SingleTableBuilder {
        private Map<String, Object> fieldNamesAndValues = new TreeMap<>(); //TreeMap as we want a predictable ordering

        public InsertBuilder(String tableName) {
            this(tableName, null);
        }

        public InsertBuilder(String tableName, Map<String, Object> fieldNamesAndValues) {
            super(tableName);
            withValues(fieldNamesAndValues);
        }

        public InsertBuilder withValues(Map<String, Object> fieldNamesAndValues) {
            this.fieldNamesAndValues.clear();
            if (fieldNamesAndValues != null) {
                this.fieldNamesAndValues.putAll(fieldNamesAndValues);
            }
            return this;
        }

        public InsertBuilder addValue(String fieldName, Object value) {
            this.fieldNamesAndValues.put(fieldName, value);
            return this;
        }

        @Override
        public BuildSqlResult build() {
            final String SQL_TEMPLATE = "INSERT INTO {0} ({1}) VALUES ({2})";
            List<String> columns = new ArrayList<>(), placeholders = new ArrayList<>();
            List<Object> bindValues = new ArrayList<>();
            this.fieldNamesAndValues.forEach((f, v) -> {
                columns.add(f);
                if (v instanceof ParamRawExpression) {
                    placeholders.add(((ParamRawExpression) v).expr);
                } else {
                    placeholders.add("?");
                    bindValues.add(v);
                }
            });
            String columnsStr = StringUtils.join(columns.toArray(), ",");
            String placeholdersStr = StringUtils.join(placeholders.toArray(), ",");
            String sql = MessageFormat.format(SQL_TEMPLATE, tableName, columnsStr, placeholdersStr);
            return new BuildSqlResult(sql, bindValues.toArray());
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link ISqlBuilder} that builds SELECT statement.
     *
     * <p>Built statement: {@code SELECT <column-list> FROM <table-list> [WHERE <where-filter>] [GROUP BY <group-by-columns>] [HAVING <having-filter>] [ORDER BY <sorting>] [limits]}</p>
     */
    public static class SelectBuilder extends MultipleTablesBuilder {
        private List<String> columns = new ArrayList<>();
        private IFilter filterWhere;
        private List<String> groupByColumns = new ArrayList<>();
        private IFilter filterHaving;
        private int limitNumRows = 0, startOffset = 0;
        private Map<String, Boolean> sorting = new LinkedHashMap<>(); //LinkedHashMap as we want ordering of insertion

        public SelectBuilder withColumns(Collection<String> columnNames) {
            this.columns.clear();
            if (columnNames != null) {
                this.columns.addAll(columnNames);
            }
            return this;
        }

        public SelectBuilder withColumns(String... columnNames) {
            this.columns.clear();
            if (columnNames != null) {
                for (String columnName : columnNames) {
                    this.columns.add(columnName);
                }
            }
            return this;
        }

        public SelectBuilder addColumn(String columnName) {
            this.columns.add(columnName);
            return this;
        }

        /**
         * Filter for "WHERE" clause.
         *
         * @param filter
         * @return
         */
        public SelectBuilder withFilterWhere(IFilter filter) {
            this.filterWhere = filter;
            return this;
        }

        public SelectBuilder withGroupByColumns(Collection<String> columnNames) {
            this.groupByColumns.clear();
            if (columnNames != null) {
                this.groupByColumns.addAll(columnNames);
            }
            return this;
        }

        public SelectBuilder withGroupByColumns(String... columnNames) {
            this.groupByColumns.clear();
            if (columnNames != null) {
                for (String columnName : columnNames) {
                    this.groupByColumns.add(columnName);
                }
            }
            return this;
        }

        public SelectBuilder addGroupByColumn(String columnName) {
            this.groupByColumns.add(columnName);
            return this;
        }

        /**
         * Filter for "HAVING" clause.
         *
         * @param filter
         * @return
         */
        public SelectBuilder withFilterHaving(IFilter filter) {
            this.filterHaving = filter;
            return this;
        }

        public SelectBuilder withLimit(int limitNumRows) {
            this.limitNumRows = limitNumRows;
            return this;
        }

        public SelectBuilder withLimit(int limitNumRows, int startOffset) {
            this.limitNumRows = limitNumRows;
            this.startOffset = startOffset;
            return this;
        }

        /**
         * Set sorting parameters (to build the ORDER BY clause).
         *
         * <p>Note: the order of sorting fields depends on the input parameters. It maybe un-predictable.</p>
         *
         * @param sorting parameters in form of {@code [fieldName:is_descending]}
         * @return
         */
        public SelectBuilder withSorting(Map<String, Boolean> sorting) {
            this.sorting.clear();
            if (sorting != null) {
                this.sorting.putAll(sorting);
            }
            return this;
        }

        /**
         * Add a sorting parameter (to build the ORDER BY clause). Sorting fields are ordered based on time they are added
         * (e.g. {@code addSorting("columnB", false).addSorting("columnA", true)} will generate clause {@code ORDER BY columnB ASC, columnA DESC}).
         *
         * @param columnName
         * @param descending
         * @return
         */
        public SelectBuilder addSorting(String columnName, boolean descending) {
            this.sorting.put(columnName, descending ? Boolean.TRUE : Boolean.FALSE);
            return this;
        }

        @Override
        public BuildSqlResult build() {
            StringBuilder sql = new StringBuilder(MessageFormat
                    .format("SELECT {0} FROM {1}", StringUtils.join(columns.toArray(), ","),
                            StringUtils.join(tableNames.toArray(), ",")));
            List<Object> bindValues = new ArrayList<>();

            BuildSqlResult whereResult = filterWhere != null ? filterWhere.build() : null;
            if (whereResult != null) {
                sql.append(" WHERE ").append(whereResult.clause);
                for (Object obj : whereResult.bindValues) {
                    bindValues.add(obj);
                }
            }

            String groupBy = StringUtils.join(groupByColumns.toArray(), ",");
            if (!StringUtils.isBlank(groupBy)) {
                sql.append(" GROUP BY ").append(groupBy);
            }

            BuildSqlResult havingResult = filterHaving != null ? filterHaving.build() : null;
            if (havingResult != null) {
                sql.append(" HAVING ").append(havingResult.clause);
                for (Object obj : havingResult.bindValues) {
                    bindValues.add(obj);
                }
            }

            List<String> orderByList = new ArrayList<>();
            this.sorting.forEach((k, v) -> orderByList.add(k + (v ? " DESC" : " ASC")));
            String orderBy = StringUtils.join(orderByList.toArray(), ",");
            if (!StringUtils.isBlank(orderBy)) {
                sql.append(" ORDER BY ").append(orderBy);
            }

            if (limitNumRows != 0) {
                switch (vendor) {
                case POSTGRESQL:
                    sql.append(" LIMIT ").append(limitNumRows);
                    if (startOffset != 0) {
                        sql.append(" OFFSET ").append(startOffset);
                    }
                    break;
                case MSSQL:
                    if (sorting.size() > 0) {
                        // available since SQL Server 2012 && Azure SQL Database
                        if (startOffset != 0) {
                            sql.append(" OFFSET ").append(startOffset).append(" ROWS");
                        }
                        sql.append(" FETCH NEXT ").append(limitNumRows).append(" ROWS ONLY");
                    }
                    break;
                case ORACLE:
                    if (startOffset != 0) {
                        sql.append(" OFFSET ").append(startOffset).append(" ROWS");
                    }
                    sql.append(" FETCH NEXT ").append(limitNumRows).append(" ROWS ONLY");
                    break;
                case MYSQL:
                default:
                    sql.append(" LIMIT ");
                    if (startOffset != 0) {
                        sql.append(startOffset).append(",");
                    }
                    sql.append(limitNumRows);
                    break;
                }
            }

            return new BuildSqlResult(sql.toString(), bindValues.toArray());
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link ISqlBuilder} that builds UPDATE statement.
     *
     * <p>Built statement: {@code UPDATE <table-name> SET <update-clause> [WHERE <filter>]}</p>
     */
    public static class UpdateBuilder extends SingleTableBuilder {
        private IFilter filter;
        private Map<String, Object> fieldNamesAndValues = new TreeMap<>(); //TreeMap as we want a predictable ordering

        public UpdateBuilder(String tableName) {
            super(tableName);
        }

        public UpdateBuilder(String tableName, Map<String, Object> fieldNamesAndValues, IFilter filter) {
            super(tableName);
            withValues(fieldNamesAndValues);
            withFilter(filter);
        }

        /**
         * Filter for "WHERE" clause.
         *
         * @param filter
         * @return
         */
        public UpdateBuilder withFilter(IFilter filter) {
            this.filter = filter;
            return this;
        }

        public UpdateBuilder withValues(Map<String, Object> fieldNamesAndValues) {
            this.fieldNamesAndValues.clear();
            if (fieldNamesAndValues != null) {
                this.fieldNamesAndValues.putAll(fieldNamesAndValues);
            }
            return this;
        }

        public UpdateBuilder addValue(String fieldName, Object value) {
            this.fieldNamesAndValues.put(fieldName, value);
            return this;
        }

        @Override
        public BuildSqlResult build() {
            StringBuilder sql = new StringBuilder().append("UPDATE ").append(tableName);
            List<Object> bindValues = new ArrayList<>();
            List<String> updateClause = new ArrayList<>();
            this.fieldNamesAndValues.forEach((f, v) -> {
                if (v instanceof ParamRawExpression) {
                    updateClause.add(f + "=" + ((ParamRawExpression) v).expr);
                } else {
                    updateClause.add(f + "=?");
                    bindValues.add(v);
                }
            });
            sql.append(" SET ").append(StringUtils.join(updateClause.toArray(), ","));
            BuildSqlResult whereClause = filter != null ? filter.build() : null;
            if (whereClause != null) {
                sql.append(" WHERE ").append(whereClause.clause);
                for (Object obj : whereClause.bindValues) {
                    bindValues.add(obj);
                }
            }
            return new BuildSqlResult(sql.toString(), bindValues.
                    toArray());
        }
    }
}
