package com.github.ddth.dao.jdbc.utils;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.1.0
 */
public class NamedParamUtils {
    private final static String separatorChars;

    static {
        String cq = "";
        for (int i = 1; i <= 31; i++) {
            cq += (char) i;
        }
        separatorChars = cq;
    }

    /**
     * Split the supplied input into field name and parameter names.
     *
     * @param input in format {@code <field-name>[<separator><param-name>]} where {@code separator} is a character sequence in range {@code \u0001-\u001f}
     * @return an array of 1 or 2 elements, where the fist element is {@code field-name} and the second one is {@code param-name}
     */
    public static String[] splitFieldAndParamNames(String input) {
        return input != null ? StringUtils.split(input.trim(), separatorChars) : null;
    }

    //    /**
    //     * Extract the parameter name from supplied name.
    //     *
    //     * @param input in format {@code <field-name>[<separator><param-name>]} where {@code separator} is a character sequence in range {@code \u0001-\u001f}
    //     * @return the {@code param-name} part if exists, otherwise {@code field-name}
    //     */
    //    public static String extractParamName(String input) {
    //        String[] tokens = splitFieldAndParamNames(input);
    //        return tokens != null ? (tokens.length > 1 ? tokens[1] : tokens[0]).trim() : null;
    //    }
}
