package com.github.ddth.dao.utils;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;

/**
 * Root class for ddth-dao exceptions.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class DaoException extends RuntimeException {
    private static final long serialVersionUID = "0.8.0".hashCode();

    public DaoException() {
    }

    public DaoException(String message) {
        super(message);
    }

    public DaoException(Throwable cause) {
        super(cause);
    }

    public DaoException(String message, Throwable cause) {
        super(message, cause);
    }

    public DaoException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    /*----------------------------------------------------------------------*/
    /**
     * Translate to {@link DaoException}.
     *
     * @param dae
     * @return
     */
    public static DaoException translate(DataAccessException dae) {
        if (dae instanceof DuplicateKeyException) {
            return new DuplicatedValueException(dae);
        }
        return new DaoException(dae);
    }
}
