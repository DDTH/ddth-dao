package com.github.ddth.dao;

/**
 * Captures profiling record of a storage action.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.1.0
 */

public class ProfilingRecord {
    public final static ProfilingRecord[] EMPTY_ARRAY = new ProfilingRecord[0];

    public long execTime;
    public String command;

    public ProfilingRecord() {
    }

    public ProfilingRecord(long execTime, String command) {
        this.execTime = execTime;
        this.command = command;
    }
}
