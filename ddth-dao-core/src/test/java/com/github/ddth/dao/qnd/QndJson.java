package com.github.ddth.dao.qnd;

import java.util.HashMap;
import java.util.Map;

import com.github.ddth.commons.utils.SerializationUtils;

public class QndJson {
    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<>();
        map.put("key", "value".getBytes());
        String json = SerializationUtils.toJsonString(map);
        System.out.println(map);
        System.out.println(json);

        Object objBack1 = SerializationUtils.fromJsonString(json);
        System.out.println(objBack1);

        byte[] data = SerializationUtils.toByteArrayFst(map);
        System.out.println(data.length);
        Object objBack2 = SerializationUtils.fromByteArrayFst(data);
        System.out.println(objBack2);
    }
}
