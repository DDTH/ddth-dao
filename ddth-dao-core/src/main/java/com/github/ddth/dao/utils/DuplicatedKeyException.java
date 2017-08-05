package com.github.ddth.dao.utils;

/**
 * Thrown to indicate that a storage write results in violation of primary key
 * constraint.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class DuplicatedKeyException extends DaoException {
    private static final long serialVersionUID = "0.8.0".hashCode();

    public DuplicatedKeyException() {
    }

    public DuplicatedKeyException(String message) {
        super(message);
    }

    public DuplicatedKeyException(Throwable cause) {
        super(cause);
    }

    public DuplicatedKeyException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicatedKeyException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
