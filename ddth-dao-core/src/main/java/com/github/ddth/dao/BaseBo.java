package com.github.ddth.dao;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.github.ddth.commons.serialization.DeserializationException;
import com.github.ddth.commons.serialization.ISerializationSupport;
import com.github.ddth.commons.serialization.SerializationException;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.commons.utils.SerializationUtils;

/**
 * Base class for application Business Objects (BO).
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class BaseBo implements Cloneable, ISerializationSupport {

    protected <T> Map<String, T> initAttributes(Map<String, T> initData) {
        return initData != null ? new ConcurrentHashMap<String, T>(initData)
                : new ConcurrentHashMap<String, T>();
    }

    private Map<String, Object> attributes = initAttributes(null);

    private boolean dirty = false;

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.0.5
     */
    public BaseBo clone() {
        lock.lock();
        try {
            BaseBo obj = (BaseBo) super.clone();
            obj.attributes = initAttributes(attributes);
            obj.dirty = dirty;
            obj.lock = new ReentrantLock();
            return obj;
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Has the BO been changed?
     * 
     * @return
     */
    public boolean isDirty() {
        return dirty;
    }

    /**
     * Mark that the BO is dirty.
     * 
     * @return
     */
    protected BaseBo markDirty() {
        dirty = true;
        return this;
    }

    /**
     * Mark that the BO is no longer dirty.
     * 
     * @return
     */
    public BaseBo markClean() {
        dirty = false;
        return this;
    }

    /**
     * Check an attribute exists.
     * 
     * @param attrName
     * @return
     * @since 0.7.1
     */
    protected boolean attributeExists(String attrName) {
        return attributes.containsKey(attrName);
    }

    /**
     * Return the underlying attribute map.
     * 
     * @return
     * @since 0.7.1
     */
    protected Map<String, Object> attributeMap() {
        return attributes;
    }

    /**
     * Get a BO's attribute.
     * 
     * @param attrName
     * @return
     */
    public Object getAttribute(String attrName) {
        return attributes.get(attrName);
    }

    /**
     * Get a BO's attribute.
     * 
     * @param attrName
     * @param clazz
     * @return
     */
    public <T> T getAttribute(String attrName, Class<T> clazz) {
        return MapUtils.getValue(attributes, attrName, clazz);
    }

    /**
     * Get a BO's attribute.
     * 
     * @param attrName
     * @param clazz
     * @return
     * @since 0.8.0
     */
    public <T> Optional<T> getAttributeOptional(String attrName, Class<T> clazz) {
        return Optional.ofNullable(MapUtils.getValue(attributes, attrName, clazz));
    }

    /**
     * Get a BO's attribute as a date. If the attribute value is a string, parse
     * it as a {@link Date} using the specified date-time format.
     * 
     * @param attrName
     * @param dateTimeFormat
     * @return
     * @since 0.8.0
     */
    public Date getAttributeAsDate(String attrName, String dateTimeFormat) {
        return MapUtils.getDate(attributes, attrName, dateTimeFormat);
    }

    /**
     * Set a BO's attribute.
     * 
     * @param attrName
     * @param value
     * @return
     */
    public BaseBo setAttribute(String attrName, Object value) {
        return setAttribute(attrName, value, true);
    }

    /**
     * Set a BO's attribute.
     * 
     * @param attrName
     * @param value
     * @param triggerChange
     *            if set to {@code true} {@link #triggerChange(String)} will be
     *            called
     * @return
     * @since 0.7.1
     */
    public BaseBo setAttribute(String attrName, Object value, boolean triggerChange) {
        lock.lock();
        try {
            if (value == null) {
                attributes.remove(attrName);
            } else {
                attributes.put(attrName, value);
            }
            if (triggerChange) {
                triggerChange(attrName);
            }
            markDirty();
            return this;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove a BO's attribute.
     * 
     * @param attrName
     * @return
     * @since 0.7.1
     */
    protected BaseBo removeAttribute(String attrName) {
        return removeAttribute(attrName, true);
    }

    /**
     * Remove a BO's attribute.
     * 
     * @param attrName
     * @param triggerChange
     *            if set to {@code true} {@link #triggerChange(String)} will be
     *            called
     * @return
     * @since 0.7.1
     */
    protected BaseBo removeAttribute(String attrName, boolean triggerChange) {
        lock.lock();
        try {
            attributes.remove(attrName);
            if (triggerChange) {
                triggerChange(attrName);
            }
            markDirty();
            return this;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Called when the BO's entire attribute set are (re)populated.
     * 
     * @since 0.5.0.2
     */
    protected void triggerPopulate() {
        // EMPTY
    }

    /**
     * Called when one of BO's attributes is changed (added, updated or
     * removed).
     * 
     * @param attrName
     * @since 0.7.1
     */
    protected void triggerChange(String attrName) {
        // EMPTY
    }

    private Lock lock = new ReentrantLock();

    public final static String SER_FIELD_ATTRS = "_attrs_";
    public final static String SER_FIELD_DIRTY = "_dirty_";

    /**
     * Lock the BO for synchronization.
     * 
     * @since 0.7.1
     */
    protected void lock() {
        lock.lock();
    }

    /**
     * Try to lock the BO for synchronization.
     * 
     * @return
     * @since 0.7.1
     */
    protected boolean tryLock() {
        return lock.tryLock();
    }

    /**
     * Try to lock the BO for synchronization.
     * 
     * @param time
     * @param unit
     * @return
     * @throws InterruptedException
     * @since 0.7.1
     */
    protected boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lock.tryLock(time, unit);
    }

    /**
     * @since 0.7.1
     */
    protected void unlock() {
        lock.unlock();
    }

    /**
     * Populate the BO with data from a Java map.
     * 
     * @param data
     * @return
     */
    @SuppressWarnings("unchecked")
    public BaseBo fromMap(Map<String, Object> data) {
        if (data != null) {
            lock.lock();
            try {
                Boolean dirty = DPathUtils.getValue(data, SER_FIELD_DIRTY, Boolean.class);
                Map<String, Object> attrs = DPathUtils.getValue(data, SER_FIELD_ATTRS, Map.class);
                this.attributes = initAttributes(attrs);
                this.dirty = dirty != null ? dirty.booleanValue() : true;
                triggerPopulate();
            } finally {
                lock.unlock();
            }
        }
        return this;
    }

    /**
     * Serialize the BO to a Java map.
     * 
     * @return
     */
    public Map<String, Object> toMap() {
        lock.lock();
        try {
            Map<String, Object> data = new HashMap<>();
            data.put(SER_FIELD_DIRTY, dirty);
            data.put(SER_FIELD_ATTRS,
                    attributes != null ? new HashMap<>(attributes) : new HashMap<>());
            return data;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Populate the BO with data from a JSON string (previously generated by
     * {@link #toJson()}.
     * 
     * @param jsonString
     * @return
     */
    @SuppressWarnings("unchecked")
    public BaseBo fromJson(String jsonString) {
        Map<String, Object> dataMap = null;
        try {
            dataMap = jsonString != null ? SerializationUtils.fromJsonString(jsonString, Map.class)
                    : null;
        } catch (Exception e) {
            dataMap = null;
        }
        return fromMap(dataMap);
    }

    /**
     * Serialize the BO to JSON string.
     * 
     * @return
     */
    public String toJson() {
        Map<String, Object> data = toMap();
        return SerializationUtils.toJsonString(data);
    }

    /**
     * Populate the BO with data from a byte array (previously generated by
     * {@link #toByteArray()}.
     * 
     * @param data
     * @return
     * @since 0.5.0
     */
    @SuppressWarnings("unchecked")
    public BaseBo fromByteArray(byte[] data) {
        Map<String, Object> dataMap = null;
        try {
            dataMap = data != null ? SerializationUtils.fromByteArray(data, Map.class) : null;
        } catch (Exception e) {
            dataMap = null;
        }
        return fromMap(dataMap);
    }

    /**
     * Serialize the BO to byte array.
     * 
     * @return
     * @since 0.5.0
     */
    public byte[] toByteArray() {
        Map<String, Object> dataMap = toMap();
        return SerializationUtils.toByteArray(dataMap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof BaseBo) {
            BaseBo other = (BaseBo) obj;
            EqualsBuilder eb = new EqualsBuilder();
            lock.lock();
            try {
                other.lock.lock();
                try {
                    eb.append(attributes, other.attributes);
                    return eb.isEquals();
                } finally {
                    other.lock.unlock();
                }
            } finally {
                lock.unlock();
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        lock.lock();
        try {
            hcb.append(attributes);
        } finally {
            lock.unlock();
        }
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        return toJson();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public byte[] toBytes() throws SerializationException {
        return toByteArray();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public ISerializationSupport fromBytes(byte[] data) throws DeserializationException {
        return fromByteArray(data);
    }
}
