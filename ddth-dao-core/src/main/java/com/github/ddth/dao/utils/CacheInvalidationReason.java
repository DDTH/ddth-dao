package com.github.ddth.dao.utils;

/**
 * Reason to invalidate cache entry.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public enum CacheInvalidationReason {
    /**
     * Cache item is invalidated because a new item has been created.
     */
    CREATE(0),

    /**
     * Cache item is invalidated because an item has been updated/modified.
     */
    UPDATE(1),

    /**
     * Cache item is invalidated because an item has been deleted/removed.
     */
    DELETE(2);

    private final int value;

    CacheInvalidationReason(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
