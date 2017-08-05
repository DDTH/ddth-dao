package com.github.ddth.dao.utils;

/**
 * Thrown to indicate that a storage write results in violation of unique index
 * constraint.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class DuplicatedUniqueException extends DaoException {
    private static final long serialVersionUID = "0.8.0".hashCode();

    public DuplicatedUniqueException() {
    }

    public DuplicatedUniqueException(String message) {
        super(message);
    }

    public DuplicatedUniqueException(Throwable cause) {
        super(cause);
    }

    public DuplicatedUniqueException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicatedUniqueException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
