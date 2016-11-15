package com.github.ddth.dao;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

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
            Constructor<T> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            T t = constructor.newInstance();
            t.fromJson(json);
            return t;
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
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
            Constructor<T> constructor = clazz.getDeclaredConstructor();
            constructor.setAccessible(true);
            T t = constructor.newInstance();
            t.fromBytes(data);
            return t;
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException
                | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new DeserializationException(e);
        }
    }
}
