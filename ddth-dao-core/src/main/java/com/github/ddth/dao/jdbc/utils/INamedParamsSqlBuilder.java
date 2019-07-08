package com.github.ddth.dao.jdbc.utils;

/**
 * Abstract interface to build SQL statements with named-parameters.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.1.0
 */
public interface INamedParamsSqlBuilder {
    /**
     * Build the SQL statement with named placeholders, along with bind values.
     *
     * @return
     */
    BuildNamedParamsSqlResult build();
}
