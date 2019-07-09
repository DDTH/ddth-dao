package com.github.ddth.dao.jdbc.utils;

import com.github.ddth.dao.utils.DatabaseVendor;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.*;

/**
 * Default implementations of {@link INamedParamsSqlBuilder}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.1.0
 */
public class DefaultNamedParamsSqlBuilders {
    /**
     * Base class for {@link INamedParamsSqlBuilder} implementations.
     */
    public static abstract class BaseBuilder implements INamedParamsSqlBuilder {
        private DatabaseVendor vendor = DatabaseVendor.UNKNOWN;

        public DatabaseVendor getVendor() {
            return vendor;
        }

        /**
         * Specify the database vendor.
         *
         * @param vendor
         * @return
         */
        public BaseBuilder withVendor(DatabaseVendor vendor) {
            this.vendor = vendor != null ? vendor : DatabaseVendor.UNKNOWN;
            return this;
        }
    }

    /**
     * Build SQL statement that queries on one single database table.
     */
    public static abstract class SingleTableBuilder extends BaseBuilder {
        private String tableName;

        public SingleTableBuilder() {
        }

        public SingleTableBuilder(String tableName) {
            this.tableName = tableName;
        }

        public String getTableName() {
            return tableName;
        }

        /**
         * Specify the database table name.
         *
         * @param tableName
         * @return
         */
        public SingleTableBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }
    }

    /**
     * Build SQL statement that queries on multiple database tables.
     */
    public static abstract class MultipleTablesBuilder extends BaseBuilder {
        protected List<String> tableNames = new ArrayList<>();

        public MultipleTablesBuilder() {
        }

        public MultipleTablesBuilder(Collection<String> tableNames) {
            withTableNames(tableNames);
        }

        protected List<String> getTableNames() {
            return tableNames;
        }

        public MultipleTablesBuilder withTableNames(Collection<String> tableNames) {
            this.tableNames.clear();
            if (tableNames != null) {
                this.tableNames.addAll(tableNames);
            }
            return this;
        }

        public MultipleTablesBuilder withTableNames(String... tableNames) {
            this.tableNames.clear();
            if (tableNames != null) {
                for (String tableName : tableNames) {
                    this.tableNames.add(tableName);
                }
            }
            return this;
        }

        public MultipleTablesBuilder addTableName(String tableName) {
            this.tableNames.add(tableName);
            return this;
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link INamedParamsSqlBuilder} that builds DELETE statement.
     *
     * <p>Built statement: {@code DELETE FROM <table-name> [WHERE <filter>]}</p>
     */
    public static class DeleteBuilder extends SingleTableBuilder {
        private INamedParamsFilter filter;

        public DeleteBuilder() {
        }

        public DeleteBuilder(String tableName, INamedParamsFilter filter) {
            super(tableName);
            this.filter = filter;
        }

        protected INamedParamsFilter getFilter() {
            return filter;
        }

        /**
         * Specify the filter.
         *
         * @param filter
         * @return
         */
        public DeleteBuilder withFilter(INamedParamsFilter filter) {
            this.filter = filter;
            return this;
        }

        @Override
        public BuildNamedParamsSqlResult build() {
            final String SQL_TEMPLATE_FULL = "DELETE FROM {0} WHERE {1}";
            final String SQL_TEMPLATE = "DELETE FROM {0}";
            if (filter == null) {
                return new BuildNamedParamsSqlResult(MessageFormat.format(SQL_TEMPLATE, getTableName()), null);
            } else {
                BuildNamedParamsSqlResult whereClause = filter.build();
                return new BuildNamedParamsSqlResult(
                        MessageFormat.format(SQL_TEMPLATE_FULL, getTableName(), whereClause.clause),
                        whereClause.bindValues);
            }
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link INamedParamsSqlBuilder} that builds INSERT statement.
     *
     * <p>Built statement: {@code INSERT INTO <table-name> (<column-list>) VALUES (<placeholders>)}</p>
     */
    public static class InsertBuilder extends SingleTableBuilder {
        private Map<String, Object> fieldNamesAndValues = new TreeMap<>(); //TreeMap as we want a predictable ordering

        public InsertBuilder() {
        }

        public InsertBuilder(String tableName) {
            this(tableName, null);
        }

        /**
         * @param tableName
         * @param fieldNamesAndValues each fieldName is encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         * @see NamedParamUtils#splitFieldAndParamNames(String)
         */
        public InsertBuilder(String tableName, Map<String, Object> fieldNamesAndValues) {
            super(tableName);
            withValues(fieldNamesAndValues);
        }

        /**
         * @return each fieldName is encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         */
        protected Map<String, Object> getFieldNamesAndValues() {
            return fieldNamesAndValues;
        }

        /**
         * @param fieldNamesAndValues each fieldName is encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         * @return
         */
        public InsertBuilder withValues(Map<String, Object> fieldNamesAndValues) {
            this.fieldNamesAndValues.clear();
            if (fieldNamesAndValues != null) {
                this.fieldNamesAndValues.putAll(fieldNamesAndValues);
            }
            return this;
        }

        /**
         * @param fieldName encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         * @param value
         * @return
         */
        public InsertBuilder addValue(String fieldName, Object value) {
            this.fieldNamesAndValues.put(fieldName, value);
            return this;
        }

        @Override
        public BuildNamedParamsSqlResult build() {
            final String SQL_TEMPLATE = "INSERT INTO {0} ({1}) VALUES ({2})";
            List<String> columns = new ArrayList<>(), placeholders = new ArrayList<>();
            Map<String, Object> bindValues = new HashMap<>();
            this.fieldNamesAndValues.forEach((f, v) -> {
                String[] tokens = NamedParamUtils.splitFieldAndParamNames(f);
                columns.add(tokens[0]);
                if (v instanceof ParamRawExpression) {
                    placeholders.add(((ParamRawExpression) v).expr);
                } else {
                    String paramName = tokens.length > 1 ? tokens[1] : tokens[0];
                    placeholders.add(":" + paramName);
                    bindValues.put(paramName, v);
                }
            });
            String columnsStr = StringUtils.join(columns.toArray(), ",");
            String placeholdersStr = StringUtils.join(placeholders.toArray(), ",");
            String sql = MessageFormat.format(SQL_TEMPLATE, getTableName(), columnsStr, placeholdersStr);
            return new BuildNamedParamsSqlResult(sql, bindValues);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link INamedParamsSqlBuilder} that builds SELECT statement.
     *
     * <p>Built statement: {@code SELECT <column-list> FROM <table-list> [WHERE <where-filter>] [GROUP BY <group-by-columns>] [HAVING <having-filter>] [ORDER BY <sorting>] [limits]}</p>
     */
    public static class SelectBuilder extends MultipleTablesBuilder {
        private List<String> columns = new ArrayList<>();
        private INamedParamsFilter filterWhere;
        private List<String> groupByColumns = new ArrayList<>();
        private INamedParamsFilter filterHaving;
        private int limitNumRows = 0, startOffset = 0;
        private Map<String, Boolean> sorting = new LinkedHashMap<>(); //LinkedHashMap as we want ordering of insertion

        protected List<String> getColumns() {
            return columns;
        }

        protected INamedParamsFilter getFilterWhere() {
            return filterWhere;
        }

        protected List<String> getGroupByColumns() {
            return groupByColumns;
        }

        protected INamedParamsFilter getFilterHaving() {
            return filterHaving;
        }

        protected int getLimitNumRows() {
            return limitNumRows;
        }

        protected int getStartOffset() {
            return startOffset;
        }

        protected Map<String, Boolean> getSorting() {
            return sorting;
        }

        /**
         * Specify list of column names.
         *
         * @param columnNames
         * @return
         */
        public SelectBuilder withColumns(Collection<String> columnNames) {
            this.columns.clear();
            if (columnNames != null) {
                this.columns.addAll(columnNames);
            }
            return this;
        }

        /**
         * Specify list of column names.
         *
         * @param columnNames
         * @return
         */
        public SelectBuilder withColumns(String... columnNames) {
            this.columns.clear();
            if (columnNames != null) {
                for (String columnName : columnNames) {
                    this.columns.add(columnName);
                }
            }
            return this;
        }

        /**
         * Add a column name to the list.
         *
         * @param columnName
         * @return
         */
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
        public SelectBuilder withFilterWhere(INamedParamsFilter filter) {
            this.filterWhere = filter;
            return this;
        }

        /**
         * Specify list of column names for "group by" clause.
         *
         * @param columnNames
         * @return
         */
        public SelectBuilder withGroupByColumns(Collection<String> columnNames) {
            this.groupByColumns.clear();
            if (columnNames != null) {
                this.groupByColumns.addAll(columnNames);
            }
            return this;
        }

        /**
         * Specify list of column names for "group by" clause.
         *
         * @param columnNames
         * @return
         */
        public SelectBuilder withGroupByColumns(String... columnNames) {
            this.groupByColumns.clear();
            if (columnNames != null) {
                for (String columnName : columnNames) {
                    this.groupByColumns.add(columnName);
                }
            }
            return this;
        }

        /**
         * Add a column name to the list.
         *
         * @param columnName
         * @return
         */
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
        public SelectBuilder withFilterHaving(INamedParamsFilter filter) {
            this.filterHaving = filter;
            return this;
        }

        /**
         * Specify value(s) for "limit" clause.
         *
         * @param limitNumRows
         * @return
         */
        public SelectBuilder withLimit(int limitNumRows) {
            this.limitNumRows = limitNumRows;
            return this;
        }

        /**
         * Specify value(s) for "limit" clause.
         *
         * @param limitNumRows
         * @param startOffset
         * @return
         */
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
        public BuildNamedParamsSqlResult build() {
            StringBuilder sql = new StringBuilder(MessageFormat
                    .format("SELECT {0} FROM {1}", StringUtils.join(columns.toArray(), ","),
                            StringUtils.join(tableNames.toArray(), ",")));
            Map<String, Object> bindValues = new HashMap<>();

            BuildNamedParamsSqlResult whereResult = filterWhere != null ? filterWhere.build() : null;
            if (whereResult != null) {
                sql.append(" WHERE ").append(whereResult.clause);
                bindValues.putAll(whereResult.bindValues);
            }

            String groupBy = StringUtils.join(groupByColumns.toArray(), ",");
            if (!StringUtils.isBlank(groupBy)) {
                sql.append(" GROUP BY ").append(groupBy);
            }

            BuildNamedParamsSqlResult havingResult = filterHaving != null ? filterHaving.build() : null;
            if (havingResult != null) {
                sql.append(" HAVING ").append(havingResult.clause);
                bindValues.putAll(whereResult.bindValues);
            }

            List<String> orderByList = new ArrayList<>();
            this.sorting.forEach((k, v) -> orderByList.add(k + (v ? " DESC" : " ASC")));
            String orderBy = StringUtils.join(orderByList.toArray(), ",");
            if (!StringUtils.isBlank(orderBy)) {
                sql.append(" ORDER BY ").append(orderBy);
            }

            if (limitNumRows != 0) {
                DatabaseVendor vendor = getVendor();
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

            return new BuildNamedParamsSqlResult(sql.toString(), bindValues);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@link INamedParamsSqlBuilder} that builds UPDATE statement.
     *
     * <p>Built statement: {@code UPDATE <table-name> SET <update-clause> [WHERE <filter>]}</p>
     */
    public static class UpdateBuilder extends SingleTableBuilder {
        private INamedParamsFilter filter;
        private Map<String, Object> fieldNamesAndValues = new TreeMap<>(); //TreeMap as we want a predictable ordering

        public UpdateBuilder() {
        }

        public UpdateBuilder(String tableName) {
            super(tableName);
        }

        /**
         * @param tableName
         * @param fieldNamesAndValues each fieldName is encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         * @param filter
         */
        public UpdateBuilder(String tableName, Map<String, Object> fieldNamesAndValues, INamedParamsFilter filter) {
            super(tableName);
            withValues(fieldNamesAndValues);
            withFilter(filter);
        }

        protected INamedParamsFilter getFilter() {
            return filter;
        }

        /**
         * @return each fieldName is encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         */
        protected Map<String, Object> getFieldNamesAndValues() {
            return fieldNamesAndValues;
        }

        /**
         * Filter for "WHERE" clause.
         *
         * @param filter
         * @return
         */
        public UpdateBuilder withFilter(INamedParamsFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * @param fieldNamesAndValues each fieldName is encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         * @return
         */
        public UpdateBuilder withValues(Map<String, Object> fieldNamesAndValues) {
            this.fieldNamesAndValues.clear();
            if (fieldNamesAndValues != null) {
                this.fieldNamesAndValues.putAll(fieldNamesAndValues);
            }
            return this;
        }

        /**
         * @param fieldName encoded as {@code <field-name>[<separator><param-name>]} and will be split into {@code field-name} and {@code param-name}
         * @param value
         * @return
         */
        public UpdateBuilder addValue(String fieldName, Object value) {
            this.fieldNamesAndValues.put(fieldName, value);
            return this;
        }

        @Override
        public BuildNamedParamsSqlResult build() {
            StringBuilder sql = new StringBuilder().append("UPDATE ").append(getTableName());
            Map<String, Object> bindValues = new HashMap<>();
            List<String> updateClause = new ArrayList<>();
            this.fieldNamesAndValues.forEach((f, v) -> {
                String[] tokens = NamedParamUtils.splitFieldAndParamNames(f);
                if (v instanceof ParamRawExpression) {
                    updateClause.add(tokens[0] + "=" + ((ParamRawExpression) v).expr);
                } else {
                    String paramName = tokens.length > 1 ? tokens[1] : tokens[0];
                    updateClause.add(f + "=:" + paramName);
                    bindValues.put(paramName, v);
                }
            });
            sql.append(" SET ").append(StringUtils.join(updateClause.toArray(), ","));
            BuildNamedParamsSqlResult whereClause = filter != null ? filter.build() : null;
            if (whereClause != null) {
                sql.append(" WHERE ").append(whereClause.clause);
                bindValues.putAll(whereClause.bindValues);
            }
            return new BuildNamedParamsSqlResult(sql.toString(), bindValues);
        }
    }
}
