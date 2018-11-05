package com.github.ddth.dao.qnd.nosql.lucene;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class BaseNosqlLuceneQnd {

    static {
        System.setProperty("org.slf4j.simpleLogger.logFile", "System.out");
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "INFO");
        System.setProperty("org.slf4j.simpleLogger.showThreadName", "false");
        System.setProperty("org.slf4j.simpleLogger.showLogName", "false");
        System.setProperty("org.slf4j.simpleLogger.showShortLogName", "false");
    }

    protected static Directory directory(boolean cleanup) throws IOException {
        File dir = new File("./temp");
        if (cleanup) {
            FileUtils.deleteQuietly(dir);
        }
        dir.mkdirs();

        return FSDirectory.open(dir.toPath());
    }
}
