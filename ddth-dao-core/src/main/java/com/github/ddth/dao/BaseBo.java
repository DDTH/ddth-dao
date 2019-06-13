package com.github.ddth.dao;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.ddth.commons.serialization.DeserializationException;
import com.github.ddth.commons.serialization.ISerializationSupport;
import com.github.ddth.commons.serialization.SerializationException;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.HashUtils;
import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.commons.utils.SerializationUtils;

/**
 * Base class for application Business Objects (BO).
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class BaseBo implements Cloneable, ISerializationSupport {

    /**
     * Deep-clone data.
     * 
     * @param data
     * @return
     * @since 0.10.0
     */
    @SuppressWarnings("unchecked")
    protected static <T> Map<String, T> cloneData(Map<String, T> data) {
        return SerializationUtils.fromByteArrayFst(SerializationUtils.toByteArrayFst(data), Map.class);
    }

    protected <T> Map<String, T> initAttributes(Map<String, T> initData) {
        return initData != null ? new ConcurrentHashMap<>(cloneData(initData))
                : new ConcurrentHashMap<>();
    }

    private Map<String, Object> attributes = initAttributes(null);

    private boolean dirty = false;

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.0.5
     */
    public BaseBo clone() {
        Lock lock = lockForWrite();
        try {
            BaseBo obj = (BaseBo) super.clone();
            obj.attributes = initAttributes(attributes);
            obj.dirty = dirty;
            obj.rwLock = new ReentrantReadWriteLock(true);
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

    /*----------------------------------------------------------------------*/

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
     * Get all BO's attributes as a map.
     * 
     * @return
     * @since 0.8.2
     */
    public Map<String, Object> getAttributes() {
        if (attributes == null) {
            return null;
        }
        Lock lock = lockForRead();
        try {
            return cloneData(attributes);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set all BO's attributes.
     * 
     * @param attrs
     * @return
     */
    public BaseBo setAttributes(Map<String, Object> attrs) {
        Lock lock = lockForWrite();
        try {
            attributes = initAttributes(attrs);
            triggerPopulate();
            return this;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get all BO's attributes as a {@link JsonNode}.
     * 
     * @return BO's attributes as a {@link JsonNode}, {@link NullNode} is returned if BO's
     *         attribute-map is null
     * @since 0.10.0
     */
    public JsonNode getAttributesAsJson() {
        if (attributes == null) {
            return NullNode.instance;
        }
        Lock lock = lockForRead();
        try {
            return SerializationUtils.toJson(attributes);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set all BO's attributes.
     * 
     * @param attrs
     * @return
     * @since 0.10.0
     */
    @SuppressWarnings("unchecked")
    public BaseBo setAttributes(JsonNode attrs) {
        if (attrs == null || attrs instanceof NullNode || attrs instanceof MissingNode) {
            return setAttributes((Map<String, Object>) null);
        }
        if (attrs instanceof ObjectNode) {
            return setAttributes(SerializationUtils.fromJson(attrs, Map.class));
        }
        throw new IllegalArgumentException(
                "Argument must be of type NullNode, MissingNode or ObjectNode");
    }

    /**
     * Get all BO's attributes as a JSON-string.
     * 
     * @return
     * @since 0.10.0
     */
    public String getAttributesAsJsonString() {
        if (attributes == null) {
            return "null";
        }
        Lock lock = lockForRead();
        try {
            return SerializationUtils.toJsonString(attributes);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Set all BO's attributes.
     * 
     * @param jsonString
     * @return
     * @since 0.10.0
     */
    @SuppressWarnings("unchecked")
    public BaseBo setAttributes(String jsonString) {
        if (StringUtils.isBlank(jsonString)) {
            return setAttributes((Map<String, Object>) null);
        }
        return setAttributes(SerializationUtils.fromJsonString(jsonString, Map.class));
    }

    /*----------------------------------------------------------------------*/

    /**
     * Get a BO's attribute.
     * 
     * @param attrName
     * @return
     */
    public Object getAttribute(String attrName) {
        Lock lock = lockForRead();
        try {
            return attributes != null ? attributes.get(attrName) : null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get a BO's attribute.
     * 
     * @param attrName
     * @param clazz
     * @return
     */
    public <T> T getAttribute(String attrName, Class<T> clazz) {
        Lock lock = lockForRead();
        try {
            return MapUtils.getValue(attributes, attrName, clazz);
        } finally {
            lock.unlock();
        }
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
        return Optional.ofNullable(getAttribute(attrName, clazz));
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
        Lock lock = lockForRead();
        try {
            return MapUtils.getDate(attributes, attrName, dateTimeFormat);
        } finally {
            lock.unlock();
        }
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
        Lock lock = lockForWrite();
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
        Lock lock = lockForWrite();
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
        checksum = null;
    }

    /**
     * Called when one of BO's attributes is changed (added, updated or
     * removed).
     * 
     * @param attrName
     * @since 0.7.1
     */
    protected void triggerChange(String attrName) {
        checksum = null;
    }

    private Long checksum = null;

    /**
     * Calculate the checksum of BO's attributes (ignore "dirty" flag).
     * 
     * @return
     * @since 0.10.0
     */
    public long calcChecksum() {
        if (checksum == null) {
            Lock lock = lockForRead();
            try {
                checksum = checksum();
            } finally {
                lock.unlock();
            }
        }
        return checksum.longValue();
    }

    /**
     * Sub-class may override this method to implement its own business logic.
     * 
     * <p>
     * This method is called by {@link #calcChecksum()}, no need to implement lock/synchronization
     * </p>
     * 
     * @return
     * @since 0.10.0
     */
    protected long checksum() {
        return HashUtils.checksum(attributes, HashUtils.murmur3);
    }

    /*----------------------------------------------------------------------*/

    /**
     * @since 0.10.0
     */
    private ReadWriteLock rwLock = new ReentrantReadWriteLock(true);

    /**
     * Obtain the BO's "read" lock.
     * 
     * @return
     * @since 0.10.0
     */
    protected Lock readLock() {
        return rwLock.readLock();
    }

    /**
     * Obtain the BO's "write" lock.
     * 
     * @return
     * @since 0.10.0
     */
    protected Lock writeLock() {
        return rwLock.writeLock();
    }

    /**
     * Lock the BO for read.
     * 
     * @since 0.10.0
     * @return the "read"-lock in "lock" state
     */
    protected Lock lockForRead() {
        Lock lock = readLock();
        lock.lock();
        return lock;
    }

    /**
     * Try to lock the BO for read.
     * 
     * @return the "read"-lock in "lock" state if successful, {@code null} otherwise
     * @since 0.10.0
     */
    protected Lock tryLockForRead() {
        Lock lock = readLock();
        return lock.tryLock() ? lock : null;
    }

    /**
     * Try to lock the BO for read.
     * 
     * @param time
     * @param unit
     * @return the "read"-lock in "lock" state if successful, {@code null} otherwise
     * @throws InterruptedException
     * @since 0.10.0
     */
    protected Lock tryLockForRead(long time, TimeUnit unit) throws InterruptedException {
        if (time < 0 || unit == null) {
            return tryLockForRead();
        }
        Lock lock = readLock();
        return lock.tryLock(time, unit) ? lock : null;
    }

    /**
     * Release the "read"-lock.
     * 
     * @since 0.10.0
     */
    protected void unlockForRead() {
        readLock().unlock();
    }

    /**
     * Lock the BO for write.
     * 
     * @since 0.10.0
     * @return the "write"-lock in "lock" state
     */
    protected Lock lockForWrite() {
        Lock lock = writeLock();
        lock.lock();
        return lock;
    }

    /**
     * Try to lock the BO for write.
     * 
     * @return the "write"-lock in "lock" state if successful, {@code null} otherwise
     * @since 0.10.0
     */
    protected Lock tryLockForWrite() {
        Lock lock = writeLock();
        return lock.tryLock() ? lock : null;
    }

    /**
     * Try to lock the BO for write.
     * 
     * @param time
     * @param unit
     * @return the "write"-lock in "lock" state if successful, {@code null} otherwise
     * @throws InterruptedException
     * @since 0.10.0
     */
    protected Lock tryLockForWrite(long time, TimeUnit unit) throws InterruptedException {
        if (time < 0 || unit == null) {
            return tryLockForWrite();
        }
        Lock lock = writeLock();
        return lock.tryLock(time, unit) ? lock : null;
    }

    /**
     * Release the "write"-lock.
     * 
     * @since 0.10.0
     */
    protected void unlockForWrite() {
        writeLock().unlock();
    }

    /*----------------------------------------------------------------------*/

    public final static String SER_FIELD_ATTRS = "_attrs_";
    public final static String SER_FIELD_DIRTY = "_dirty_";

    /**
     * De-serialize the BO from a Java map (previously generated by {@link #toMap()}.
     * 
     * <p>
     * This method is meant to be used in conjunction with method {@code #toMap()}. Use
     * {@link #setAttributes(Map)} to reset just the underlying attribute map.
     * </p>
     * 
     * @param data
     *            BO data as a Java map (generated via {@link #toMap()}
     * @return
     */
    @SuppressWarnings("unchecked")
    public BaseBo fromMap(Map<String, Object> data) {
        if (data != null) {
            Lock lock = lockForWrite();
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
     * @return BO's data as a Java map, can be used to de-serialize the BO via {@link #fromMap(Map)}
     */
    public Map<String, Object> toMap() {
        Lock lock = lockForWrite();
        try {
            Map<String, Object> data = new HashMap<>();
            data.put(SER_FIELD_DIRTY, dirty);
            data.put(SER_FIELD_ATTRS, attributes != null ? cloneData(attributes) : new HashMap<>());
            return data;
        } finally {
            lock.unlock();
        }
    }

    /**
     * De-serialize the BO from a JSON string (previously generated by {@link #toJson()}.
     * 
     * @param jsonString
     *            BO data as a JSON string (generated via {@link #toJson()}
     * 
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
     * @return BO's data as a JSON string, can be used to de-serialize the BO via
     *         {@link #fromJson(String)}
     */
    public String toJson() {
        Map<String, Object> data = toMap();
        return SerializationUtils.toJsonString(data);
    }

    /**
     * De-serialize the BO from a byte array (previously generated by {@link #toByteArray()}.
     * 
     * @param data
     *            BO data as a byte array (generated via {@link #toByteArray()}
     * @return
     * @since 0.5.0
     */
    @SuppressWarnings("unchecked")
    public BaseBo fromByteArray(byte[] data) {
        Map<String, Object> dataMap = null;
        try {
            dataMap = data != null ? SerializationUtils.fromByteArrayFst(data, Map.class) : null;
        } catch (Exception e) {
            dataMap = null;
        }
        return fromMap(dataMap);
    }

    /**
     * Serialize the BO to byte array.
     * 
     * @return BO's data as a byte array, can be used to de-serialize the BO via
     *         {@link #fromByteArray(byte[])}
     * @since 0.5.0
     */
    public byte[] toByteArray() {
        Map<String, Object> dataMap = toMap();
        return SerializationUtils.toByteArrayFst(dataMap);
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
            Lock thisLock = lockForRead();
            try {
                Lock otherLock = other.lockForRead();
                try {
                    eb.append(attributes, other.attributes);
                    return eb.isEquals();
                } finally {
                    otherLock.unlock();
                }
            } finally {
                thisLock.unlock();
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        Lock lock = lockForRead();
        try {
            HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
            hcb.append(attributes);
            return hcb.hashCode();
        } finally {
            lock.unlock();
        }
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
