package com.github.ddth.dao;

import java.util.HashMap;
import java.util.Map;
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

    protected Map<String, Object> initAttributes(Map<String, Object> initData) {
        return initData != null ? new ConcurrentHashMap<String, Object>(initData)
                : new ConcurrentHashMap<String, Object>();
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
     * Marks that the BO is dirty.
     * 
     * @return
     */
    protected BaseBo markDirty() {
        dirty = true;
        return this;
    }

    /**
     * Marks that the BO is no longer dirty.
     * 
     * @return
     */
    public BaseBo markClean() {
        dirty = false;
        return this;
    }

    /**
     * Checks an attribute exists.
     * 
     * @param attrName
     * @return
     * @since 0.7.1
     */
    protected boolean attributeExists(String attrName) {
        return attributes.containsKey(attrName);
    }

    /**
     * Returns the underlying attribute map.
     * 
     * @return
     * @since 0.7.1
     */
    protected Map<String, Object> attributeMap() {
        return attributes;
    }

    /**
     * Gets a BO's attribute.
     * 
     * @param attrName
     * @return
     */
    protected Object getAttribute(String attrName) {
        return attributes.get(attrName);
    }

    /**
     * Gets a BO's attribute.
     * 
     * @param attrName
     * @param clazz
     * @return
     */
    protected <T> T getAttribute(String attrName, Class<T> clazz) {
        return MapUtils.getValue(attributes, attrName, clazz);
    }

    /**
     * Sets a BO's attribute.
     * 
     * @param attrName
     * @param value
     * @return
     */
    protected BaseBo setAttribute(String attrName, Object value) {
        return setAttribute(attrName, value, true);
    }

    /**
     * Sets a BO's attribute.
     * 
     * @param attrName
     * @param value
     * @param triggerChange
     *            if set to {@code true} {@link #triggerChange(String)} will be
     *            called
     * @return
     * @since 0.7.1
     */
    protected BaseBo setAttribute(String attrName, Object value, boolean triggerChange) {
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
     * Removes a BO's attribute.
     * 
     * @param attrName
     * @return
     * @since 0.7.1
     */
    protected BaseBo removeAttribute(String attrName) {
        return removeAttribute(attrName, true);
    }

    /**
     * Removes a BO's attribute.
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

    private final static String SER_FIELD_ATTRS = "_attrs_";
    private final static String SER_FIELD_DIRTY = "_dirty_";

    /**
     * Locks the BO for synchronization.
     * 
     * @since 0.7.1
     */
    protected void lock() {
        lock.lock();
    }

    /**
     * Tries to lock the BO for synchronization.
     * 
     * @return
     * @since 0.7.1
     */
    protected boolean tryLock() {
        return lock.tryLock();
    }

    /**
     * Tries to lock the BO for synchronization.
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
     * Populates the BO with data from a Java map.
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
     * Serializes the BO to a Java map.
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
     * Populates the BO with data from a JSON string (previously generated by
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
     * Serializes the BO to JSON string.
     * 
     * @return
     */
    public String toJson() {
        Map<String, Object> data = toMap();
        return SerializationUtils.toJsonString(data);
    }

    /**
     * Populates the BO with data from a byte array (previously generated by
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
     * Serializes the BO to byte array.
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
