package com.github.ddth.dao.utils;

/**
 * Thrown to indicate that a storage write results in violation of primary key or unique index
 * constraint.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.2
 */
public class DuplicatedValueException extends DaoException {
    private static final long serialVersionUID = "0.8.2".hashCode();

    public DuplicatedValueException() {
    }

    public DuplicatedValueException(String message) {
        super(message);
    }

    public DuplicatedValueException(Throwable cause) {
        super(cause);
    }

    public DuplicatedValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicatedValueException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
