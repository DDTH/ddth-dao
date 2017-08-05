package com.github.ddth.dao.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.commons.serialization.DeserializationException;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.dao.BaseBo;

/**
 * BO utility class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0.1
 */
public class BoUtils {

    private final static String FIELD_CLASSNAME = "c";
    private final static String FIELD_BODATA = "bo";

    @SuppressWarnings("unchecked")
    public static <T> T createObject(String className, ClassLoader classLoader,
            Class<T> clazzToCast) throws InstantiationException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
            SecurityException, ClassNotFoundException {
        Class<?> clazz = classLoader != null ? Class.forName(className, false, classLoader)
                : Class.forName(className);
        Constructor<?> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object result = constructor.newInstance();
        return result != null && clazz.isAssignableFrom(result.getClass()) ? (T) result : null;
    }

    /**
     * Serializes a BO to JSON string.
     * 
     * @param bo
     * @return
     */
    public static String toJson(BaseBo bo) {
        if (bo == null) {
            return null;
        }
        String clazz = bo.getClass().getName();
        Map<String, Object> data = new HashMap<>();
        data.put(FIELD_CLASSNAME, clazz);
        data.put(FIELD_BODATA, bo.toJson());
        return SerializationUtils.toJsonString(data);
    }

    /**
     * Deserializes a BO from a JSON string.
     * 
     * @param json
     *            the JSON string obtained from {@link #toJson(BaseBo)}
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromJson(String json) {
        return fromJson(json, BaseBo.class, null);
    }

    /**
     * Deserializes a BO from a JSON string.
     * 
     * @param json
     *            the JSON string obtained from {@link #toJson(BaseBo)}
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromJson(String json, ClassLoader classLoader) {
        return fromJson(json, BaseBo.class, classLoader);
    }

    /**
     * Deserializes a BO from a JSON string.
     * 
     * @param json
     *            the JSON string obtained from {@link #toJson(BaseBo)}
     * @param clazz
     * @return
     */
    public static <T extends BaseBo> T fromJson(String json, Class<T> clazz) {
        return fromJson(json, clazz, null);
    }

    /**
     * Deserializes a BO from a JSON string.
     * 
     * @param json
     *            the JSON string obtained from {@link #toJson(BaseBo)}
     * @param clazz
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    @SuppressWarnings("unchecked")
    public static <T extends BaseBo> T fromJson(String json, Class<T> clazz,
            ClassLoader classLoader) {
        if (StringUtils.isBlank(json) || clazz == null) {
            return null;
        }
        try {
            Map<String, Object> data = SerializationUtils.fromJsonString(json, Map.class);
            String boClassName = DPathUtils.getValue(data, FIELD_CLASSNAME, String.class);
            T bo = createObject(boClassName, classLoader, clazz);
            if (bo != null) {
                bo.fromJson(DPathUtils.getValue(data, FIELD_BODATA, String.class));
                return bo;
            } else {
                return null;
            }
        } catch (Exception e) {
            throw e instanceof DeserializationException ? (DeserializationException) e
                    : new DeserializationException(e);
        }
    }

    /**
     * Serializes a BO to a byte array.
     * 
     * @param bo
     * @return
     */
    public static byte[] toBytes(BaseBo bo) {
        if (bo == null) {
            return null;
        }
        String clazz = bo.getClass().getName();
        Map<String, Object> data = new HashMap<>();
        data.put(FIELD_CLASSNAME, clazz);
        data.put(FIELD_BODATA, bo.toBytes());
        return SerializationUtils.toByteArray(data);
    }

    /**
     * Deserializes a BO from a byte array.
     * 
     * @param bytes
     *            the byte array obtained from {@link #toBytes(BaseBo)}
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromBytes(byte[] bytes) {
        return fromBytes(bytes, BaseBo.class, null);
    }

    /**
     * Deserializes a BO from a byte array.
     * 
     * @param bytes
     *            the byte array obtained from {@link #toBytes(BaseBo)}
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromBytes(byte[] bytes, ClassLoader classLoader) {
        return fromBytes(bytes, BaseBo.class, classLoader);
    }

    /**
     * Deserializes a BO from a byte array.
     * 
     * @param bytes
     *            the byte array obtained from {@link #toBytes(BaseBo)}
     * @param clazz
     * @return
     */
    public static <T extends BaseBo> T fromBytes(byte[] bytes, Class<T> clazz) {
        return fromBytes(bytes, clazz, null);
    }

    /**
     * Deserializes a BO from a byte array.
     * 
     * @param bytes
     *            the byte array obtained from {@link #toBytes(BaseBo)}
     * @param clazz
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    @SuppressWarnings("unchecked")
    public static <T extends BaseBo> T fromBytes(byte[] bytes, Class<T> clazz,
            ClassLoader classLoader) {
        if (bytes == null || clazz == null) {
            return null;
        }
        try {
            Map<String, Object> data = SerializationUtils.fromByteArray(bytes, Map.class);
            String boClassName = DPathUtils.getValue(data, FIELD_CLASSNAME, String.class);
            T bo = createObject(boClassName, classLoader, clazz);
            if (bo != null) {
                bo.fromByteArray(DPathUtils.getValue(data, FIELD_BODATA, byte[].class));
                return bo;
            } else {
                return null;
            }
        } catch (Exception e) {
            throw e instanceof DeserializationException ? (DeserializationException) e
                    : new DeserializationException(e);
        }
    }
}
