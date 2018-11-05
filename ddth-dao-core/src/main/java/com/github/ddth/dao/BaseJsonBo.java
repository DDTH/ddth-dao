package com.github.ddth.dao;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.ddth.commons.serialization.SerializationException;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.HashUtils;
import com.github.ddth.commons.utils.JacksonUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.commons.utils.ValueUtils;

/**
 * Similar to {@link BaseBo} but each attribute is JSON-encoded string (or a {@link JsonNode}. If an
 * attribute is a map or list, sub-attributes are accessed using d-path (see
 * {@link DPathUtils}).
 * 
 * <ul>
 * <li>{@link #setAttribute(String, Object)} and {@link #setAttribute(String, Object, boolean)}
 * convert the {@code input value} to JSON-encoded string.</li>
 * <li>If {@link #setAttribute(String, Object)} and {@link #setAttribute(String, Object, boolean)}
 * detect that the {@code input value}
 * is already in JSON-encoded format, the value is used as-is.</li>
 * </ul>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class BaseJsonBo extends BaseBo {
    protected Map<String, JsonNode> cacheJsonObjs = initAttributes(null);

    /**
     * {@inheritDoc}
     * 
     * @since 0.10.0
     */
    @Override
    public BaseJsonBo setAttribute(String attrName, Object value) {
        return setAttribute(attrName, value, true);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.10.0
     */
    @Override
    public BaseJsonBo setAttribute(String attrName, Object value, boolean triggerChange) {
        if (value instanceof JsonNode) {
            if (value instanceof NullNode || value instanceof MissingNode) {
                super.setAttribute(attrName, null, triggerChange);
            } else {
                super.setAttribute(attrName, value.toString(), triggerChange);
            }
        } else {
            JsonNode node = null;
            if (value instanceof String) {
                node = tryParseJson(value.toString());
            }
            node = node != null ? node : SerializationUtils.toJson(value);
            super.setAttribute(attrName, node.toString(), triggerChange);
        }
        return this;
    }

    private JsonNode tryParseJson(String input) {
        try {
            return SerializationUtils.readJson(input);
        } catch (SerializationException e) {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public JsonNode getAttribute(String attrName) {
        Lock lock = lockForRead();
        try {
            return cacheJsonObjs.get(attrName);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T getAttribute(String attrName, Class<T> clazz) {
        Lock lock = lockForRead();
        try {
            return JacksonUtils.asValue(getAttribute(attrName), clazz);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> Optional<T> getAttributeOptional(String attrName, Class<T> clazz) {
        return Optional.ofNullable(getAttribute(attrName, clazz));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getAttributeAsDate(String attrName, String dateTimeFormat) {
        Lock lock = lockForRead();
        try {
            return ValueUtils.convertDate(getAttribute(attrName), dateTimeFormat);
        } finally {
            lock.unlock();
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * Get a sub-attribute using d-path.
     * 
     * @param attrName
     * @param dPath
     * @return
     * @see DPathUtils
     */
    public JsonNode getSubAttr(String attrName, String dPath) {
        Lock lock = lockForRead();
        try {
            return JacksonUtils.getValue(getAttribute(attrName), dPath);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get a sub-attribute using d-path.
     * 
     * @param attrName
     * @param dPath
     * @param clazz
     * @return
     * @see DPathUtils
     */
    public <T> T getSubAttr(String attrName, String dPath, Class<T> clazz) {
        Lock lock = lockForRead();
        try {
            return JacksonUtils.getValue(getAttribute(attrName), dPath, clazz);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get a sub-attribute using d-path.
     * 
     * @param attrName
     * @param dPath
     * @param clazz
     * @return
     * @since 0.8.0
     */
    public <T> Optional<T> getSubAttrOptional(String attrName, String dPath, Class<T> clazz) {
        return Optional.ofNullable(getSubAttr(attrName, dPath, clazz));
    }

    /**
     * Get a sub-attribute as a date using d-path. If sub-attr's value is a
     * string, parse it as a {@link Date} using the specified date-time format.
     * 
     * @param attrName
     * @param dPath
     * @param dateTimeFormat
     * @return
     */
    public Date getSubAttrAsDate(String attrName, String dPath, String dateTimeFormat) {
        Lock lock = lockForRead();
        try {
            return JacksonUtils.getDate(getAttribute(attrName), dPath, dateTimeFormat);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set a sub-attribute.
     * 
     * @param attrName
     * @param value
     * @return
     */
    public BaseJsonBo setSubAttr(String attrName, String dPath, Object value) {
        if (value == null) {
            return removeSubAttr(attrName, dPath);
        }
        Lock lock = lockForWrite();
        try {
            JsonNode attr = cacheJsonObjs.get(attrName);
            if (attr == null) {
                // initialize the first chunk
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
            lock.unlock();
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
        Lock lock = lockForWrite();
        try {
            JsonNode attr = cacheJsonObjs.get(attrName);
            JacksonUtils.deleteValue(attr, dPath);
            return (BaseJsonBo) setAttribute(attrName, SerializationUtils.toJsonString(attr),
                    false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerChange(String attrName) {
        super.triggerChange(attrName);
        Lock lock = lockForRead();
        try {
            Object value = super.getAttribute(attrName);
            if (value == null) {
                cacheJsonObjs.remove(attrName);
            } else {
                JsonNode node = value instanceof JsonNode ? (JsonNode) value
                        : SerializationUtils.readJson(value.toString());
                cacheJsonObjs.put(attrName, node);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerPopulate() {
        super.triggerPopulate();
        Lock lock = lockForRead();
        try {
            cacheJsonObjs.clear();
            Map<String, Object> attributeMap = attributeMap();
            if (attributeMap != null) {
                attributeMap.forEach((k, v) -> {
                    if (v != null) {
                        JsonNode node = v instanceof JsonNode ? (JsonNode) v
                                : SerializationUtils.readJson(v.toString());
                        cacheJsonObjs.put(k, node);
                    }
                });
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.10.0
     */
    @Override
    protected long checksum() {
        return HashUtils.checksum(cacheJsonObjs, HashUtils.murmur3);
    }
}
