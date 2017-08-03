package com.github.ddth.dao.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.dao.BoUtils;

/**
 * Generic implementation of {@link IRowMapper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 *
 * @param <T>
 * @since 0.8.0
 */
public class BaseRowMapper<T extends BaseBo> implements IRowMapper<T> {

    private final Class<T> typeClass;

    public BaseRowMapper(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        try {
            T bo = BoUtils.createObject(typeClass.getName(), null, typeClass);
            return bo;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException
                | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

}
