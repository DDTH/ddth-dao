package com.github.ddth.dao.qnd.nosql.cassandra;

import com.github.ddth.cql.SessionManager;

public class BaseNosqlCassandraQnd {
    
    static {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showLogName", "false");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "false");
    }
    
    protected static SessionManager sessionManager() {
        SessionManager sm = new SessionManager();
        sm.setDefaultHostsAndPorts("localhost").setDefaultUsername("test")
                .setDefaultPassword("test").setDefaultKeyspace(null).init();
        return sm;
    }
}
