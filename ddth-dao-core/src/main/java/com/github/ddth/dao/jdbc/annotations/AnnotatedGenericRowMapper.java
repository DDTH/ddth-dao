package com.github.ddth.dao.jdbc.annotations;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;

import com.github.ddth.dao.jdbc.AbstractGenericRowMapper;
import com.github.ddth.dao.jdbc.IRowMapper;

/**
 * Abstract generic implementation of {@link IRowMapper}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 *
 * @param <T>
 * @since 0.8.0
 */
public class AnnotatedGenericRowMapper<T> extends AbstractGenericRowMapper<T> {

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getPrimaryKeyColumns() {
        PrimaryKeyColumns[] pkCols = getClass().getAnnotationsByType(PrimaryKeyColumns.class);
        return pkCols != null && pkCols.length > 0 ? pkCols[0].columns()
                : ArrayUtils.EMPTY_STRING_ARRAY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getUpdateColumns() {
        UpdateColumns[] updateCols = getClass().getAnnotationsByType(UpdateColumns.class);
        return updateCols != null && updateCols.length > 0 ? updateCols[0].columns()
                : ArrayUtils.EMPTY_STRING_ARRAY;
    }

    private Map<String, ColAttrMapping> cachedColumnAttributeMappings;

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, ColAttrMapping> getColumnAttributeMappings() {
        if (cachedColumnAttributeMappings == null) {
            cachedColumnAttributeMappings = new HashMap<>();
            ColumnAttribute[] annoMappings = getClass().getAnnotationsByType(ColumnAttribute.class);
            for (ColumnAttribute colAttr : annoMappings) {
                cachedColumnAttributeMappings.put(colAttr.column(),
                        new ColAttrMapping(colAttr.column(), colAttr.attr(), colAttr.attrClass()));
            }
        }
        return cachedColumnAttributeMappings;
    }

}
