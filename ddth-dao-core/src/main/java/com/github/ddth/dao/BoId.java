package com.github.ddth.dao;

import java.util.Arrays;

/**
 * Represent a BO's id.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.8.0
 */
public class BoId {
    public final Object[] values;

    public BoId(Object singleValue) {
        this.values = Arrays.asList(singleValue).toArray();
    }

    public BoId(Object[] values) {
        this.values = Arrays.asList(values).toArray();
    }

    public Object[] getValues() {
        return values;
    }
}
