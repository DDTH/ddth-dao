package com.github.ddth.dao.utils;

/**
 * Reason to invalidate cache entry.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public enum CacheInvalidationReason {
    CREATE(0), UPDATE(1), DELETE(2);

    private final int value;

    CacheInvalidationReason(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
