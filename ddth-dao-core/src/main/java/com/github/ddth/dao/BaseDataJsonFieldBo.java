package com.github.ddth.dao;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;

/**
 * Similar to {@link BaseBo}, but there is one special "data" field which is
 * JSON-encoded.
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

    public BaseDataJsonFieldBo setData(String data) {
        setAttribute(ATTR_DATA, data != null ? data.trim() : "{}");
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerChange(String attrName) {
        if (StringUtils.equals(attrName, ATTR_DATA)) {
            parseData();
        }
        super.triggerChange(attrName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void triggerPopulate() {
        parseData();
        super.triggerPopulate();
    }

    /**
     * Get a "data"'s sub-attribute using dPath.
     * 
     * @param dPath
     * @return
     * @see DPathUtils
     */
    public JsonNode getDataAttr(String dPath) {
        return DPathUtils.getValue(dataJson, dPath);
    }

    /**
     * Get a "data"'s sub-attribute using dPath.
     * 
     * @param dPath
     * @param clazz
     * @return
     * @see DPathUtils
     */
    public <T> T getDataAttr(String dPath, Class<T> clazz) {
        return DPathUtils.getValue(dataJson, dPath, clazz);
    }

    /**
     * Set a "data"'s sub-attribute using dPath.
     * 
     * @param dPath
     * @param value
     * @return
     * @see DPathUtils
     */
    public BaseDataJsonFieldBo setDataAttr(String dPath, Object value) {
        lock();
        try {
            DPathUtils.setValue(dataJson, dPath, value);
            return (BaseDataJsonFieldBo) setAttribute(ATTR_DATA,
                    SerializationUtils.toJsonString(dataJson), false);
        } finally {
            unlock();
        }
    }

    protected JsonNode dataJson = NullNode.instance;

    protected void parseData() {
        lock();
        try {
            dataJson = SerializationUtils.readJson(getData());
        } catch (Exception e) {
            dataJson = NullNode.instance;
        } finally {
            unlock();
        }
    }

}
