package com.github.ddth.dao.utils;

import com.github.ddth.commons.serialization.DeserializationException;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.dao.BaseBo;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * BO utility class.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0.1
 */
public class BoUtils {

    private final static String FIELD_CLASSNAME = "c";
    private final static String FIELD_BODATA = "bo";

    /**
     * Create a new object.
     *
     * @param className
     * @param classLoader
     * @param classToCast
     * @return
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     * @throws NoSuchMethodException
     * @throws SecurityException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static <T> T createObject(String className, ClassLoader classLoader, Class<T> classToCast)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException, ClassNotFoundException {
        Class<?> clazz = classLoader != null ? Class.forName(className, false, classLoader) : Class.forName(className);
        Constructor<?> constructor = clazz.getDeclaredConstructor();
        constructor.setAccessible(true);
        Object result = constructor.newInstance();
        return result != null && (classToCast == null || classToCast.isAssignableFrom(result.getClass())) ?
                (T) result :
                null;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Serialize a BO to JSON string.
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
     * De-serialize a BO from JSON string.
     *
     * @param json the JSON string obtained from {@link #toJson(BaseBo)}
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromJson(String json) {
        return fromJson(json, BaseBo.class, null);
    }

    /**
     * De-serialize a BO from JSON string.
     *
     * @param json        the JSON string obtained from {@link #toJson(BaseBo)}
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromJson(String json, ClassLoader classLoader) {
        return fromJson(json, BaseBo.class, classLoader);
    }

    /**
     * De-serialize a BO from JSON string.
     *
     * @param json  the JSON string obtained from {@link #toJson(BaseBo)}
     * @param clazz
     * @return
     */
    public static <T extends BaseBo> T fromJson(String json, Class<T> clazz) {
        return fromJson(json, clazz, null);
    }

    /**
     * De-serialize a BO from JSON string.
     *
     * @param json        the JSON string obtained from {@link #toJson(BaseBo)}
     * @param clazz
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    @SuppressWarnings("unchecked")
    public static <T extends BaseBo> T fromJson(String json, Class<T> clazz, ClassLoader classLoader) {
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
            throw e instanceof DeserializationException ?
                    (DeserializationException) e :
                    new DeserializationException(e);
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * Serialize a BO to a byte array.
     *
     * @param bo
     * @return
     */
    public static byte[] toBytes(BaseBo bo) {
        String json = toJson(bo);
        return json != null ? json.getBytes(StandardCharsets.UTF_8) : null;
    }

    /**
     * De-serialize a BO from byte array.
     *
     * @param bytes the byte array obtained from {@link #toBytes(BaseBo)}
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromBytes(byte[] bytes) {
        return fromBytes(bytes, BaseBo.class, null);
    }

    /**
     * De-serialize a BO from byte array.
     *
     * @param bytes       the byte array obtained from {@link #toBytes(BaseBo)}
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    public static BaseBo fromBytes(byte[] bytes, ClassLoader classLoader) {
        return fromBytes(bytes, BaseBo.class, classLoader);
    }

    /**
     * De-serialize a BO from byte array.
     *
     * @param bytes the byte array obtained from {@link #toBytes(BaseBo)}
     * @param clazz
     * @return
     */
    public static <T extends BaseBo> T fromBytes(byte[] bytes, Class<T> clazz) {
        return fromBytes(bytes, clazz, null);
    }

    /**
     * De-serialize a BO from byte array.
     *
     * @param bytes       the byte array obtained from {@link #toBytes(BaseBo)}
     * @param clazz
     * @param classLoader
     * @return
     * @since 0.6.0.3
     */
    public static <T extends BaseBo> T fromBytes(byte[] bytes, Class<T> clazz, ClassLoader classLoader) {
        if (bytes == null || clazz == null) {
            return null;
        }
        String json = new String(bytes, StandardCharsets.UTF_8);
        return fromJson(json, clazz, classLoader);
    }

    /**
     * De-serialize byte array to "document".
     *
     * @param data
     * @return
     * @since 0.10.0
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> bytesToDocument(byte[] data) {
        return data != null && data.length > 0 ? SerializationUtils.fromByteArrayFst(data, Map.class) : null;
    }

    /**
     * Serialize "document" to byte array.
     *
     * @param doc
     * @return
     * @since 0.10.0
     */
    public static byte[] documentToBytes(Map<String, Object> doc) {
        return doc != null ? SerializationUtils.toByteArrayFst(doc) : ArrayUtils.EMPTY_BYTE_ARRAY;
    }
}
