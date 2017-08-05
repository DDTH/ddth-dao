package com.github.ddth.dao;

import java.util.Date;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.JacksonUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.commons.utils.ValueUtils;

/**
 * Similar to {@link BaseBo} but each attribute is JSON-encoded . If an
 * attribute is a map or list, sub-attributes are accessed using DPath (see
 * {@link DPathUtils}).
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class BaseJsonBo extends BaseBo {
    private Map<String, JsonNode> cacheJsonObjs = initAttributes(null);

    /**
     * {@inheritDoc}
     */
    @Override
    public JsonNode getAttribute(String attrName) {
        return cacheJsonObjs.get(attrName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAttribute(String attrName, Class<T> clazz) {
        return JacksonUtils.asValue(getAttribute(attrName), clazz);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Optional<T> getAttributeOptional(String attrName, Class<T> clazz) {
        return Optional.ofNullable(JacksonUtils.asValue(getAttribute(attrName), clazz));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getAttributeAsDate(String attrName, String dateTimeFormat) {
        return ValueUtils.convertDate(getAttribute(attrName), dateTimeFormat);
    }

    /**
     * Get a sub-attribute using DPath.
     * 
     * @param attrName
     * @param dPath
     * @return
     * @see DPathUtils
     */
    public JsonNode getSubAttr(String attrName, String dPath) {
        return JacksonUtils.getValue(cacheJsonObjs.get(attrName), dPath);
    }

    /**
     * Get a sub-attribute using DPath.
     * 
     * @param attrName
     * @param dPath
     * @param clazz
     * @return
     * @see DPathUtils
     */
    public <T> T getSubAttr(String attrName, String dPath, Class<T> clazz) {
        return JacksonUtils.getValue(cacheJsonObjs.get(attrName), dPath, clazz);
    }

    /**
     * Get a sub-attribute using DPath.
     * 
     * @param attrName
     * @param dPath
     * @param clazz
     * @return
     * @since 0.8.0
     */
    public <T> Optional<T> getSubAttrOptional(String attrName, String dPath, Class<T> clazz) {
        return Optional
                .ofNullable(JacksonUtils.getValue(cacheJsonObjs.get(attrName), dPath, clazz));
    }

    /**
     * Get a sub-attribute as a date using DPath. If sub-attr's value is a
     * string, parse it as a {@link Date} using the specified date-time format.
     * 
     * @param attrName
     * @param dPath
     * @param dateTimeFormat
     * @return
     */
    public Date getSubAttrAsDate(String attrName, String dPath, String dateTimeFormat) {
        return JacksonUtils.getDate(cacheJsonObjs.get(attrName), dPath, dateTimeFormat);
    }

    /**
     * Set a sub-attribute.
     * 
     * @param attrName
     * @param value
     * @return
     */
    public BaseJsonBo setSubAttr(String attrName, String dPath, Object value) {
        lock();
        try {
            JsonNode attr = cacheJsonObjs.get(attrName);
            if (attr == null) {
                String[] paths = DPathUtils.splitDpath(dPath);
                if (paths[0].matches("^\\[(.*?)\\]$")) {
                    setAttribute(attrName, "[]");
                } else {
                    setAttribute(attrName, "{}");
                }
                attr = cacheJsonObjs.get(attrName);
            }
            JacksonUtils.setValue(attr, dPath, value, true);
            return (BaseJsonBo) setAttribute(attrName, SerializationUtils.toJsonString(attr),
                    false);
        } finally {
            unlock();
        }
    }

    /**
     * Remove a sub-attribute.
     * 
     * @param attrName
     * @param dPath
     * @return
     */
    public BaseJsonBo removeSubAttr(String attrName, String dPath) {
        lock();
        try {
            JsonNode attr = cacheJsonObjs.get(attrName);
            JacksonUtils.deleteValue(attr, dPath);
            return (BaseJsonBo) setAttribute(attrName, SerializationUtils.toJsonString(attr),
                    false);
        } finally {
            unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerChange(String attrName) {
        super.triggerChange(attrName);
        String value = super.getAttribute(attrName, String.class);
        if (value == null) {
            cacheJsonObjs.remove(attrName);
        } else {
            cacheJsonObjs.put(attrName, SerializationUtils.readJson(value));
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
                cacheJsonObjs.put(attrName, SerializationUtils.readJson(attrValue));
            }
        } finally {
            unlock();
        }
    }
}
