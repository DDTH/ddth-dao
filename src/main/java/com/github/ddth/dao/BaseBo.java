package com.github.ddth.dao;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;

/**
 * Base class for application Business Objects (BO).
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class BaseBo {
	@JsonProperty
	private Map<String, Object> attributes = new HashMap<String, Object>();

	/**
	 * Has the BO been changed?
	 * 
	 * @return
	 */
	@JsonIgnore
	public boolean isDirty() {
		Boolean result = getAttribute("__dirty__", Boolean.class);
		return result != null ? result.booleanValue() : false;
	}

	/**
	 * Marks that the BO is dirty.
	 * 
	 * @return
	 */
	protected BaseBo markDirty() {
		DPathUtils.setValue(attributes, "__dirty__", Boolean.TRUE);
		return this;
	}

	/**
	 * Marks that the BO is no longer dirty.
	 * 
	 * @return
	 */
	public BaseBo markClean() {
		DPathUtils.setValue(attributes, "__dirty__", Boolean.FALSE);
		return this;
	}

	/**
	 * Gets a BO's attribute.
	 * 
	 * @param dPath
	 * @return
	 * @see DPathUtils
	 */
	protected Object getAttribute(String dPath) {
		return DPathUtils.getValue(attributes, dPath);
	}

	/**
	 * Gets a BO's attribute.
	 * 
	 * @param dPath
	 * @param clazz
	 * @return
	 * @see DPathUtils
	 */
	protected <T> T getAttribute(String dPath, Class<T> clazz) {
		return DPathUtils.getValue(attributes, dPath, clazz);
	}

	/**
	 * Sets a BO's attribute.
	 * 
	 * @param dPath
	 * @param value
	 * @return
	 * @see DPathUtils
	 */
	protected BaseBo setAttribute(String dPath, Object value) {
		DPathUtils.setValue(attributes, dPath, value);
		markDirty();
		return this;
	}

	/**
	 * Populates the BO with data from a Java map.
	 * 
	 * @param data
	 * @return
	 */
	synchronized public BaseBo fromMap(Map<String, Object> data) {
		attributes = new HashMap<String, Object>();
		if (data != null) {
			attributes.putAll(data);
		}
		markClean();
		return this;
	}

	/**
	 * Serializes the BO to a Java map.
	 * 
	 * @return
	 */
	synchronized public Map<String, Object> toMap() {
		Map<String, Object> result = new HashMap<String, Object>();
		if (attributes != null) {
			result.putAll(attributes);
		}
		return result;
	}

	/**
	 * Populates the BO with data from a JSON string.
	 * 
	 * @param jsonString
	 * @return
	 */
	synchronized public BaseBo fromJson(String jsonString) {
		BaseBo other = SerializationUtils.fromJsonString(jsonString,
				BaseBo.class);
		if (other != null) {
			other.markClean();
			this.attributes = other.attributes != null ? other.attributes
					: new HashMap<String, Object>();
			return this;
		}
		return null;
	}

	/**
	 * Serializes the BO to JSON string.
	 * 
	 * @return
	 */
	synchronized public String toJson() {
		return SerializationUtils.toJsonString(this);
	}

	/**
	 * Constructs a new BO from a JSON string.
	 * 
	 * @param jsonString
	 * @param clazz
	 * @return
	 */
	public static <T extends BaseBo> T newObjectFromJson(String jsonString,
			Class<T> clazz) {
		return SerializationUtils.fromJsonString(jsonString, clazz);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof BaseBo)) {
			return false;
		}
		BaseBo other = (BaseBo) obj;
		EqualsBuilder eb = new EqualsBuilder();
		eb.append(attributes, other.attributes);
		return eb.isEquals();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int hashCode() {
		HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
		hcb.append(attributes);
		return hcb.hashCode();
	}

	/**
	 * {@inheritDoc}
	 */
	public String toString() {
		return toJson();
	}
}
