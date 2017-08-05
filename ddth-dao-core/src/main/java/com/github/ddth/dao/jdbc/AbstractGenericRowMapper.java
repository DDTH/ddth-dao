package com.github.ddth.dao.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.dao.utils.BoUtils;

/**
 * Abstract generic implementation of {@link IRowMapper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 *
 * @param <T>
 * @since 0.8.0
 */
public abstract class AbstractGenericRowMapper<T> implements IRowMapper<T> {

    /**
     * Action to extract table column data.
     */
    protected static interface ColumnDataExtractor<R> {
        public R perform(String colName) throws SQLException;
    }

    /**
     * Db table column -> BO attribute mapping.
     */
    protected static class ColAttrMapping {
        public final String colName;
        public final String attrName;
        public final Class<?> attrClass;

        public ColAttrMapping(String colName, String attrName, Class<?> attrClass) {
            this.colName = colName;
            this.attrName = attrName;
            this.attrClass = attrClass;
            setterName = "set" + StringUtils.capitalize(attrName);
            getterName = "get" + StringUtils.capitalize(attrName);
        }

        private final String setterName;
        private final String getterName;

        /**
         * Extract data from DB table column and populate to BO attribute.
         * 
         * @param bo
         * @param func
         * @throws IllegalAccessException
         * @throws IllegalArgumentException
         * @throws InvocationTargetException
         * @throws NoSuchMethodException
         * @throws SecurityException
         * @throws SQLException
         */
        public void extractColumData(Object bo, ColumnDataExtractor<?> func)
                throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
                NoSuchMethodException, SecurityException, SQLException {
            Method method = bo.getClass().getMethod(setterName, attrClass);
            method.invoke(bo, func.perform(colName));
        }

        /**
         * Extract attribute value from a BO.
         * 
         * @param bo
         * @return
         * @throws IllegalAccessException
         * @throws IllegalArgumentException
         * @throws InvocationTargetException
         * @throws NoSuchMethodException
         * @throws SecurityException
         */
        public Object extractAttrValue(Object bo)
                throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
                NoSuchMethodException, SecurityException {
            Method method = bo.getClass().getMethod(getterName);
            return method.invoke(bo);
        }

        private String cachedToString;

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            if (cachedToString == null) {
                ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
                tsb.append("column", colName).append("attr", attrName).append("attrClass",
                        attrClass);
                cachedToString = tsb.toString();
            }
            return cachedToString;
        }
    }

    private final Class<T> typeClass;

    @SuppressWarnings("unchecked")
    public AbstractGenericRowMapper() {
        ParameterizedType parameterizedType = (ParameterizedType) getClass().getGenericSuperclass();
        this.typeClass = (Class<T>) parameterizedType.getActualTypeArguments()[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T mapRow(ResultSet rs, int rowNum) throws SQLException {
        try {
            Map<String, ColAttrMapping> colAttrMappings = getColumnAttributeMappings();
            T bo = BoUtils.createObject(typeClass.getName(), null, typeClass);
            for (Entry<String, ColAttrMapping> entry : colAttrMappings.entrySet()) {
                ColAttrMapping mapping = entry.getValue();
                if (mapping.attrClass == boolean.class || mapping.attrClass == Boolean.class) {
                    mapping.extractColumData(bo, rs::getBoolean);
                } else if (mapping.attrClass == char.class || mapping.attrClass == Character.class
                        || mapping.attrClass == String.class) {
                    mapping.extractColumData(bo, rs::getString);
                } else if (mapping.attrClass == byte.class || mapping.attrClass == Byte.class) {
                    mapping.extractColumData(bo, rs::getByte);
                } else if (mapping.attrClass == short.class || mapping.attrClass == Short.class) {
                    mapping.extractColumData(bo, rs::getShort);
                } else if (mapping.attrClass == int.class || mapping.attrClass == Integer.class) {
                    mapping.extractColumData(bo, rs::getInt);
                } else if (mapping.attrClass == long.class || mapping.attrClass == Long.class
                        || mapping.attrClass == BigInteger.class) {
                    mapping.extractColumData(bo, rs::getLong);
                } else if (mapping.attrClass == float.class || mapping.attrClass == Float.class) {
                    mapping.extractColumData(bo, rs::getFloat);
                } else if (mapping.attrClass == double.class || mapping.attrClass == Double.class) {
                    mapping.extractColumData(bo, rs::getDouble);
                } else if (mapping.attrClass == BigDecimal.class) {
                    mapping.extractColumData(bo, rs::getBigDecimal);
                } else if (mapping.attrClass == byte[].class) {
                    mapping.extractColumData(bo, rs::getBytes);
                } else if (mapping.attrClass == Blob.class) {
                    mapping.extractColumData(bo, rs::getBlob);
                } else if (mapping.attrClass == Clob.class) {
                    mapping.extractColumData(bo, rs::getClob);
                } else if (mapping.attrClass == NClob.class) {
                    mapping.extractColumData(bo, rs::getNClob);
                } else if (mapping.attrClass == Date.class
                        || mapping.attrClass == Timestamp.class) {
                    mapping.extractColumData(bo, rs::getTimestamp);
                } else if (mapping.attrClass == java.sql.Date.class) {
                    mapping.extractColumData(bo, rs::getDate);
                } else if (mapping.attrClass == java.sql.Time.class) {
                    mapping.extractColumData(bo, rs::getTime);
                } else {
                    throw new IllegalArgumentException(
                            "Unsupported attribute class " + mapping.attrClass);
                }
            }
            if (bo instanceof BaseBo) {
                ((BaseBo) bo).markClean();
            }
            return bo;
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException
                | InvocationTargetException | NoSuchMethodException | SecurityException
                | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Extract attribute values from a BO for corresponding DB table columns
     * 
     * @param bo
     * @return
     */
    public Object[] valuesForColumns(T bo, String... columns) {
        Map<String, ColAttrMapping> columnAttributeMappings = getColumnAttributeMappings();
        Object[] result = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            ColAttrMapping colAttrMapping = columnAttributeMappings.get(columns[i]);
            try {
                result[i] = colAttrMapping != null ? colAttrMapping.extractAttrValue(bo) : null;
            } catch (Exception e) {
                throw e instanceof RuntimeException ? (RuntimeException) e
                        : new RuntimeException(e);
            }
        }
        return result;
    }

    private String[] cachedAllColumns;

    /**
     * Get all DB table column names.
     * 
     * @return
     */
    public String[] getAllColumns() {
        if (cachedAllColumns == null) {
            cachedAllColumns = new ArrayList<String>(getColumnAttributeMappings().keySet())
                    .toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        }
        return cachedAllColumns;
    }

    /**
     * Get DB table column names used for inserting.
     * 
     * @return
     */
    public String[] getInsertColumns() {
        return getAllColumns();
    }

    /**
     * Get primary-key column names.
     * 
     * @return
     */
    public abstract String[] getPrimaryKeyColumns();

    /**
     * Get DB table column names used for updating.
     * 
     * @return
     */
    public abstract String[] getUpdateColumns();

    /**
     * Get DB table column -> BO attribute mappings.
     * 
     * @return mappings {@code column-name -> ColAttrMapping}
     */
    public abstract Map<String, ColAttrMapping> getColumnAttributeMappings();
}
