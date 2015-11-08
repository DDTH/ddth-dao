package com.github.ddth.dao.nosql;

import java.nio.charset.Charset;

/**
 * Abstract implementation of {@link INosqlEngine}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.3.0
 */
public abstract class AbstractNosqlEngine implements INosqlEngine {
    protected final static Charset CHARSET = Charset.forName("UTF-8");

    public AbstractNosqlEngine init() {
        return this;
    }

    public void destroy() {
    }
}
