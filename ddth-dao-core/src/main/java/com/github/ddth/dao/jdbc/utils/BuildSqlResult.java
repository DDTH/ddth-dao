package com.github.ddth.dao.jdbc.utils;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Capture the result returned from {@link ISqlBuilder#build()} or {@link IFilter#build()}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class BuildSqlResult {
    /**
     * The SQL clause.
     */
    public final String clause;

    /**
     * Bind value array (if any).
     */
    public final Object[] bindValues;

    public BuildSqlResult(String clause, Object... bindValues) {
        this.clause = clause;
        this.bindValues = bindValues;
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        tsb.append("clause", clause).append("values", bindValues);
        return tsb.toString();
    }
}
