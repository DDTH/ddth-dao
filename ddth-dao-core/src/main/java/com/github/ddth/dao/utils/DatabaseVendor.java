package com.github.ddth.dao.utils;

/**
 * JDBC driver vendor info.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.3
 */
public enum DatabaseVendor {
    UNKNOWN(0), MYSQL(10), POSTGRESQL(20), MSSQL(30);

    private final int value;

    DatabaseVendor(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
