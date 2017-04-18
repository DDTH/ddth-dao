package com.github.ddth.dao;

import java.util.Map;
import java.util.Map.Entry;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;

/**
 * Similar to {@link BaseBo} but allows each attribute to be a map or list
 * stored as a JSON string. Sub-attributes are accessed using DPath (see
 * {@link DPathUtils}).
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class BaseJsonBo extends BaseBo {
    private Map<String, Object> cacheJsonObjs = initAttributes(null);

    /**
     * Gets a sub-attribute using DPath.
     * 
     * @param attrName
     * @param dPath
     * @return
     * @see DPathUtils
     */
    protected Object getSubAttr(String attrName, String dPath) {
        return DPathUtils.getValue(cacheJsonObjs.get(attrName), dPath);
    }

    /**
     * Gets a sub-attribute using DPath.
     * 
     * @param attrName
     * @param dPath
     * @param clazz
     * @return
     * @see DPathUtils
     */
    protected <T> T getSubAttr(String attrName, String dPath, Class<T> clazz) {
        return DPathUtils.getValue(cacheJsonObjs.get(attrName), dPath, clazz);
    }

    /**
     * Sets a sub-attribute.
     * 
     * @param attrName
     * @param value
     * @return
     */
    protected BaseJsonBo setSubAttr(String attrName, String dPath, Object value) {
        Object attr = cacheJsonObjs.get(attrName);
        if (attr == null) {
            String[] paths = DPathUtils.splitDpath(dPath);
            if (paths[0].matches("^\\[(.*?)\\]$")) {
                setAttribute(attrName, "[]");
            } else {
                setAttribute(attrName, "{}");
            }
            attr = cacheJsonObjs.get(attrName);
        }
        DPathUtils.setValue(attr, dPath, value, true);
        return (BaseJsonBo) setAttribute(attrName, SerializationUtils.toJsonString(attr), false);
    }

    /**
     * Removes a sub-attribute.
     * 
     * @param attrName
     * @param dPath
     * @return
     */
    protected BaseJsonBo removeSubAttr(String attrName, String dPath) {
        Object attr = cacheJsonObjs.get(attrName);
        DPathUtils.deleteValue(attr, dPath);
        return (BaseJsonBo) setAttribute(attrName, SerializationUtils.toJsonString(attr), false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerChange(String attrName) {
        super.triggerChange(attrName);
        String value = getAttribute(attrName, String.class);
        if (value == null) {
            cacheJsonObjs.remove(attrName);
        } else {
            cacheJsonObjs.put(attrName, SerializationUtils.fromJsonString(value));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerPopulate() {
        super.triggerPopulate();
        lock();
        try {
            cacheJsonObjs.clear();
            for (Entry<String, Object> entry : attributeMap().entrySet()) {
                String attrName = entry.getKey();
                String attrValue = entry.getValue().toString();
                cacheJsonObjs.put(attrName, SerializationUtils.fromJsonString(attrValue));
            }
        } finally {
            unlock();
        }
    }
}
