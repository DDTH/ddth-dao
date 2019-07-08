package com.github.ddth.dao.jdbc.utils;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Capture the result returned from {@link INamedParamsSqlBuilder#build()} or {@link INamedParamsFilter#build()}.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.1.0
 */
public class BuildNamedParamsSqlResult {
    /**
     * The SQL clause.
     */
    public final String clause;

    /**
     * Bind value map (if any).
     */
    public final Map<String, Object> bindValues;

    public BuildNamedParamsSqlResult(String clause, Map<String, Object> bindValues) {
        this.clause = clause;
        this.bindValues = Collections.unmodifiableMap(bindValues != null ? new HashMap<>(bindValues) : new HashMap<>());
    }

    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        tsb.append("clause", clause).append("values", bindValues);
        return tsb.toString();
    }
}
