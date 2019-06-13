package com.github.ddth.dao.jdbc.utils;

/**
 * Used to pass a raw expression to a SQL statement.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class ParamRawExpression {
    public final String expr;

    public ParamRawExpression(String expr) {
        this.expr = expr;
    }

    public String toString() {
        return expr;
    }
}
