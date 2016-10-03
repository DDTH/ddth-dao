package com.github.ddth.dao;

import com.github.ddth.commons.serialization.DeserializationException;

/**
 * BO utility class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0.1
 */
public class BoUtils {
    /**
     * Serializes a BO to JSON string.
     * 
     * @param bo
     * @return
     */
    public <T extends BaseBo> String toJson(T bo) {
        return bo.toJson();
    }

    /**
     * Deserializes a BO from a JSON string.
     * 
     * @param json
     * @param clazz
     * @return
     */
    public <T extends BaseBo> T fromJson(String json, Class<T> clazz) {
        try {
            T t = clazz.newInstance();
            t.fromJson(json);
            return t;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new DeserializationException(e);
        }
    }

    /**
     * Serializes a BO to a byte array.
     * 
     * @param bo
     * @return
     */
    public <T extends BaseBo> byte[] toBytes(T bo) {
        return bo.toBytes();
    }

    /**
     * Deserializes a BO from a byte array.
     * 
     * @param data
     * @param clazz
     * @return
     */
    public <T extends BaseBo> T fromBytes(byte[] data, Class<T> clazz) {
        try {
            T t = clazz.newInstance();
            t.fromBytes(data);
            return t;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new DeserializationException(e);
        }
    }
}
