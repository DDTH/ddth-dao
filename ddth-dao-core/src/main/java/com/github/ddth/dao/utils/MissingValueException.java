package com.github.ddth.dao.utils;

/**
 * Thrown to indicate that a required value is missing (e.g. write to not-null
 * column).
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class MissingValueException extends DaoException {
    private static final long serialVersionUID = "0.8.0".hashCode();

    public MissingValueException() {
    }

    public MissingValueException(String message) {
        super(message);
    }

    public MissingValueException(Throwable cause) {
        super(cause);
    }

    public MissingValueException(String message, Throwable cause) {
        super(message, cause);
    }

    public MissingValueException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
