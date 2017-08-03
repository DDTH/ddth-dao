package com.github.ddth.dao.qnd;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.ddth.commons.utils.SerializationUtils;

public class QndJson {
    public static void main(String[] args) {
        String json = "[1,2,3,4,5]";
        JsonNode jsonNode = SerializationUtils.readJson(json);
        String str = SerializationUtils.toJsonString(jsonNode);
        System.out.println(str);

        test();
    }

    public static void test(String... args) {
        System.out.println(args.length);
    }
}
