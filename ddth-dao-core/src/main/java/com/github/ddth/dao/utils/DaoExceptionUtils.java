package com.github.ddth.dao.utils;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;

/**
 * Exception-related utilities.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class DaoExceptionUtils {
    private final static String MARK_DUPLICATED_KEY = "'PRIMARY'";

    /**
     * Translate to {@link DaoException}.
     * 
     * @param dae
     * @return
     */
    public static DaoException translate(DataAccessException dae) {
        if (dae.getCause() instanceof SQLException) {
            return translate((SQLException) dae.getCause());
        }

        // if (dae instanceof DuplicateKeyException) {
        // if (StringUtils.indexOf(dae.getMessage(), MARK_DUPLICATED_KEY) >= 0)
        // {
        // return new DuplicatedKeyException(dae.getCause());
        // } else {
        // return new DuplicatedUniqueException(dae.getCause());
        // }
        // }
        // if (dae instanceof DataIntegrityViolationException) {
        // if (StringUtils.indexOf(dae.getMessage(), " null") >= 0) {
        // return new MissingValueException(dae);
        // }
        // }
        return new DaoException(dae);
    }

    /**
     * Translate to {@link DaoException}.
     * 
     * @param e
     * @return
     */
    public static DaoException translate(SQLException e) {
        if (e instanceof SQLIntegrityConstraintViolationException) {
            if (StringUtils.indexOf(e.getMessage(), MARK_DUPLICATED_KEY) >= 0) {
                return new DuplicatedKeyException(e);
            } else if (StringUtils.indexOf(e.getMessage(), " null") >= 0) {
                return new MissingValueException(e);
            } else {
                return new DuplicatedUniqueException(e);
            }
        }
        return new DaoException(e);
    }
}
