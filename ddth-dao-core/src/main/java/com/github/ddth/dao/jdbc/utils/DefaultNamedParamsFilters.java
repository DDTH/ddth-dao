package com.github.ddth.dao.jdbc.utils;

import com.github.ddth.dao.utils.DatabaseVendor;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Default implementations of {@link INamedParamsFilter}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.1.0
 */
public class DefaultNamedParamsFilters {
    /**
     * Base class for {@link INamedParamsFilter} implementations.
     *
     * @since 1.1.0
     */
    public static abstract class BaseFilter implements INamedParamsFilter {
        private DatabaseVendor vendor = DatabaseVendor.UNKNOWN;

        protected DatabaseVendor getVendor() {
            return vendor;
        }

        /**
         * Specify the database vendor.
         *
         * @param vendor
         * @return
         */
        public BaseFilter withVendor(DatabaseVendor vendor) {
            this.vendor = vendor;
            return this;
        }
    }

    /**
     * This filter combines two or more filters using an operator.
     */
    public static class FilterOptCombine extends BaseFilter {
        private String operator;
        private List<INamedParamsFilter> filters = new ArrayList<>();

        public FilterOptCombine() {
        }

        public FilterOptCombine(String operator) {
            withOperator(operator);
        }

        protected String getOperator() {
            return operator;
        }

        protected List<INamedParamsFilter> getFilters() {
            return filters;
        }

        /**
         * Set the operator.
         *
         * @param operator
         * @return
         */
        public FilterOptCombine withOperator(String operator) {
            this.operator = operator != null ? operator.trim() : null;
            return this;
        }

        /**
         * Append a filter to the list.
         *
         * @param filter
         * @return
         */
        public FilterOptCombine addFilter(INamedParamsFilter filter) {
            if (filter != null) {
                this.filters.add(filter);
            }
            return this;
        }

        /**
         * Append filters to the list.
         *
         * @param filters
         * @return
         */
        public FilterOptCombine addFilters(INamedParamsFilter... filters) {
            if (filters != null) {
                for (INamedParamsFilter filter : filters) {
                    addFilter(filter);
                }
            }
            return this;
        }

        @Override
        public BuildNamedParamsSqlResult build() {
            if (filters.size() == 0) {
                return new BuildNamedParamsSqlResult(null, null);
            }
            List<String> clause = new ArrayList<>();
            Map<String, Object> bindValues = new HashMap<>();

            BuildNamedParamsSqlResult tempResult = filters.get(0).build();
            clause.add("(" + tempResult.clause + ")");
            bindValues.putAll(tempResult.bindValues);
            filters.subList(1, filters.size()).forEach(f -> {
                BuildNamedParamsSqlResult temp = f.build();
                clause.add("(" + temp.clause + ")");
                bindValues.putAll(temp.bindValues);
            });
            String opt = " " + operator + " ";
            return new BuildNamedParamsSqlResult(StringUtils.join(clause.toArray(ArrayUtils.EMPTY_STRING_ARRAY), opt),
                    bindValues);
        }
    }

    /**
     * This filter combines two or more filters using AND clause.
     */
    public static class FilterAnd extends FilterOptCombine {
        public FilterAnd() {
            withOperator("AND");
        }

        /**
         * Set the "and" operator (default value is AND).
         *
         * @param operator
         * @return
         */
        public FilterAnd withOperator(String operator) {
            super.withOperator(StringUtils.isBlank(operator) ? "AND" : operator);
            return this;
        }
    }

    /**
     * This filter combines two or more filters using OR clause.
     */
    public static class FilterOr extends FilterOptCombine {
        public FilterOr() {
            withOperator("OR");
        }

        /**
         * Set the "or" operator (default value is OR).
         *
         * @param operator
         * @return
         */
        public FilterOr withOperator(String operator) {
            super.withOperator(StringUtils.isBlank(operator) ? "OR" : operator);
            return this;
        }
    }

    /**
     * This filter constructs a filter clause {@code <field> <operation> <value>}
     */
    public static class FilterFieldValue extends BaseFilter {
        private final String fieldName, paramName, operator;
        private final Object value;

        /**
         * {@code fieldAndParamName} will be split into {@code field-name} and {@code param-name} via {@link NamedParamUtils#splitFieldAndParamNames(String)}.
         *
         * @param fieldAndParamName
         * @param operator
         * @param value
         * @see NamedParamUtils#splitFieldAndParamNames(String)
         */
        public FilterFieldValue(String fieldAndParamName, String operator, Object value) {
            String[] tokens = NamedParamUtils.splitFieldAndParamNames(fieldAndParamName);
            this.fieldName = tokens[0];
            this.paramName = tokens.length > 1 ? tokens[1] : tokens[0];
            this.operator = operator != null ? operator.trim() : null;
            this.value = value;
        }

        protected String getFieldName() {
            return fieldName;
        }

        protected String getOperator() {
            return operator;
        }

        protected String getParamName() {
            return paramName;
        }

        protected Object getValue() {
            return value;
        }

        @Override
        public BuildNamedParamsSqlResult build() {
            if (value instanceof ParamRawExpression) {
                return new BuildNamedParamsSqlResult(
                        fieldName + " " + operator + " " + ((ParamRawExpression) value).expr, null);
            } else {
                Map<String, Object> values = new HashMap<>();
                values.put(paramName, value);
                return new BuildNamedParamsSqlResult(fieldName + " " + operator + " :" + paramName, values);
            }
        }
    }

    /**
     * This filter constructs a filter clause {@code <left_value> <operation> <right_value>}
     */
    public static class FilterExpression extends BaseFilter {
        private final String operator;
        private final Object leftValue, rightValue;
        private final String leftParamName, rightParamName;

        public FilterExpression(String leftParamName, Object leftValue, String operator, String rightParamName,
                Object rightValue) {
            this.leftParamName = leftParamName;
            this.leftValue = leftValue;
            this.operator = operator != null ? operator.trim() : null;
            this.rightParamName = rightParamName;
            this.rightValue = rightValue;
        }

        protected String getOperator() {
            return operator;
        }

        protected String getLeftParamName() {
            return leftParamName;
        }

        protected Object getLeftValue() {
            return leftValue;
        }

        protected String getRightParamName() {
            return rightParamName;
        }

        protected Object getRightValue() {
            return rightValue;
        }

        @Override
        public BuildNamedParamsSqlResult build() {
            StringBuilder clause = new StringBuilder();
            Map<String, Object> bindValues = new HashMap<>();
            if (leftValue instanceof ParamRawExpression) {
                clause.append(((ParamRawExpression) leftValue).expr);
            } else {
                clause.append(":").append(leftParamName);
                bindValues.put(leftParamName, leftValue);
            }
            clause.append(" ").append(operator).append(" ");
            if (rightValue instanceof ParamRawExpression) {
                clause.append(((ParamRawExpression) rightValue).expr);
            } else {
                clause.append(":").append(rightParamName);
                bindValues.put(rightParamName, rightValue);
            }
            return new BuildNamedParamsSqlResult(clause.toString(), bindValues);
        }
    }
}
