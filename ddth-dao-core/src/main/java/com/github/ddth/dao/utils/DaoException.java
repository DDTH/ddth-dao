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
    // private final static Pattern PATTERN_NOT_NULL = Pattern.compile("\\bnot-null\\b",
    // Pattern.CASE_INSENSITIVE);
    // private final static Pattern PATTERN_NULL = Pattern.compile("\\bnull\\b",
    // Pattern.CASE_INSENSITIVE);

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
        // if (dae instanceof DataIntegrityViolationException) {
        // String msg = dae.getMessage();
        // if (msg != null
        // && (PATTERN_NOT_NULL.matcher(msg).find() || PATTERN_NULL.matcher(msg).find())) {
        // return new MissingValueException(dae);
        // }
        // }
        return new DaoException(dae);
    }
}
