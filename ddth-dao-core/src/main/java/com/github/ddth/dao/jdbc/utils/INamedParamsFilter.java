package com.github.ddth.dao.jdbc.utils;

/**
 * Abstract interface to build SQL's "filter" statement (WHERE/HAVING/etc) with named-parameters.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.1.0
 */
public interface INamedParamsFilter {
    /**
     * Build the "filter" part (without the WHERE/HAVING/etc keyword), with mapping of named-placeholders and binding values.
     *
     * @return
     */
    BuildNamedParamsSqlResult build();
}
