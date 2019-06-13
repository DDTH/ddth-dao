package com.github.ddth.dao.jdbc.utils;

/**
 * Abstract interface to build SQL's "filter" statement (WHERE/HAVING/etc)
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public interface IFilter {
    /**
     * Build the "filter" part (without the WHERE/HAVING/etc keyword), with placeholders and list of values for binding.
     *
     * @return
     */
    BuildSqlResult build();
}
