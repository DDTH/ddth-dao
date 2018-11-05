package com.github.ddth.dao;

import java.util.Date;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.HashUtils;
import com.github.ddth.commons.utils.JacksonUtils;
import com.github.ddth.commons.utils.SerializationUtils;

/**
 * Similar to {@link BaseBo}, but there is one special "data" field which is
 * JSON-encoded.
 * 
 * <p>
 * "data" must be either {@code null} or a list or map.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class BaseDataJsonFieldBo extends BaseBo {
    protected final static String ATTR_DATA = "data";

    /**
     * Get raw value of "data" field.
     * 
     * @return
     */
    public String getData() {
        return getAttribute(ATTR_DATA, String.class);
    }

    /**
     * Set the whole "data" field.
     * 
     * @param data
     *            must be a valid JSON string
     * @return
     */
    public BaseDataJsonFieldBo setData(String data) {
        setAttribute(ATTR_DATA, data != null ? data.trim() : "{}");
        return this;
    }

    /**
     * Set the whole "data" field.
     * 
     * @param data
     * @return
     * @since 0.10.0
     */
    public BaseDataJsonFieldBo setData(JsonNode data) {
        return setData(
                data == null || data instanceof MissingNode || data instanceof NullNode ? null
                        : SerializationUtils.toJson(data));
    }

    /**
     * Set the whole "data" field.
     * 
     * @param data
     * @return
     * @since 0.10.0
     */
    public BaseDataJsonFieldBo setData(Object data) {
        return setData(data != null ? SerializationUtils.toJson(data) : null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerChange(String attrName) {
        super.triggerChange(attrName);
        if (StringUtils.equals(attrName, ATTR_DATA)) {
            parseData();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerPopulate() {
        super.triggerPopulate();
        parseData();
    }

    /*----------------------------------------------------------------------*/
    /**
     * Get the "data" field as a {@link JsonNode}.
     * 
     * @return
     * @since 0.10.0
     */
    public JsonNode getDataAttrs() {
        Lock lock = lockForRead();
        try {
            if (dataJson == null || dataJson instanceof NullNode
                    || dataJson instanceof MissingNode) {
                return null;
            }
            return dataJson.deepCopy();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get a "data"'s sub-attribute using d-path.
     * 
     * @param dPath
     * @return
     * @see DPathUtils
     */
    public JsonNode getDataAttr(String dPath) {
        Lock lock = lockForRead();
        try {
            return DPathUtils.getValue(dataJson, dPath);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get a "data"'s sub-attribute using d-path.
     * 
     * @param dPath
     * @param clazz
     * @return
     * @see DPathUtils
     */
    public <T> T getDataAttr(String dPath, Class<T> clazz) {
        Lock lock = lockForRead();
        try {
            return DPathUtils.getValue(dataJson, dPath, clazz);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get a "data"'s sub-attribute using d-path.
     * 
     * @param dPath
     * @param clazz
     * @return
     */
    public <T> Optional<T> getDataAttrOptional(String dPath, Class<T> clazz) {
        return Optional.ofNullable(getDataAttr(dPath, clazz));
    }

    /**
     * Get a "data"'s sub-attribute as date. If the attribute value is a string,
     * parse it as a {@link Date} using the specified date-time format.
     * 
     * @param dPath
     * @param dateTimeFormat
     * @return
     */
    public Date getDataAttrAsDate(String dPath, String dateTimeFormat) {
        Lock lock = lockForRead();
        try {
            return DPathUtils.getDate(dataJson, dPath, dateTimeFormat);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set a "data"'s sub-attribute using d-path.
     * 
     * @param dPath
     * @param value
     * @return
     * @see DPathUtils
     */
    public BaseDataJsonFieldBo setDataAttr(String dPath, Object value) {
        if (value == null) {
            return removeDataAttr(dPath);
        }
        Lock lock = lockForWrite();
        try {
            if (dataJson == null || dataJson instanceof MissingNode
                    || dataJson instanceof NullNode) {
                // initialize the "data"
                String[] paths = DPathUtils.splitDpath(dPath);
                if (paths[0].matches("^\\[(.*?)\\]$")) {
                    dataJson = JsonNodeFactory.instance.arrayNode();
                } else {
                    dataJson = JsonNodeFactory.instance.objectNode();
                }
            }
            JacksonUtils.setValue(dataJson, dPath, value, true);
            return (BaseDataJsonFieldBo) setAttribute(ATTR_DATA,
                    SerializationUtils.toJsonString(dataJson), false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove a "data"'s sub-attribute using d-path.
     * 
     * @param dPath
     * @return
     * @since 0.10.0
     */
    public BaseDataJsonFieldBo removeDataAttr(String dPath) {
        Lock lock = lockForWrite();
        try {
            JacksonUtils.deleteValue(dataJson, dPath);
            return (BaseDataJsonFieldBo) setAttribute(ATTR_DATA,
                    SerializationUtils.toJsonString(dataJson), false);
        } finally {
            lock.unlock();
        }
    }

    protected JsonNode dataJson = null;

    protected void parseData() {
        Lock lock = lockForRead();
        try {
            dataJson = SerializationUtils.readJson(getData());
        } catch (Exception e) {
            dataJson = null;
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
        Map<String, Object> attrs = getAttributes();
        if (attrs == null) {
            return 0;
        }
        if (dataJson != null) {
            attrs.put(ATTR_DATA, dataJson);
        }
        return HashUtils.checksum(attrs, HashUtils.murmur3);
    }
}
