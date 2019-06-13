package com.github.ddth.dao.jdbc;

/**
 * Used to pass a raw expression to a SQL statement.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 * @deprecated since 0.8.0, wait for future rework
 * @deprecated use {@link com.github.ddth.dao.jdbc.utils.ParamRawExpression} instead.
 */
@Deprecated
public class ParamExpression {

    private String expr;

    public ParamExpression(String expr) {
        this.expr = expr;
    }

    public String getExpression() {
        return expr;
    }
}
